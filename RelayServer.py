import json
import zmq
from bitshuffle import decompress_lz4
import numpy as np
import select
import time
import bitshuffle
import numpy as np
import struct

import subprocess
import sys

## ToDo: figure out why it skips recieving 1st eiger frame!
## Try if using the load balancing pattern and ROUTER/DEALER sockets solves the problem.



# def decompress_lz4(data, shape, dtype):
# 	output = np.empty(shape, dtype=dtype)
# 	nbytes_uncomp, block_size = struct.unpack('>QI', data[:12])
# 	block_size = block_size // output.itemsize
# 	arr = np.frombuffer(data[12:], dtype=np.uint8)
# 	return bitshuffle.decompress_lz4(arr, shape, dtype, block_size)

class RelayServer(object):

    def __init__(self, det_host='tcp://0.0.0.0', det_port='56789', pos_host='tcp://127.0.0.1', pos_port='5556', relay_host='tcp://127.0.0.1', relay_port='45678'):

        print(f'Starting Relay server, reading from detector address {det_host}:{det_port} and contrast address {pos_host}:{pos_port}')
        self.runpub = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Motor_streamer.py'],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
        self.runpush = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Detector_streamer.py'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        self.running = True
        self.latest_pos_index_received = -1  ## ToDo: check if these 2 should be in the beginning of check() instead
        self.latest_det_index_received = -1
        self.end_of_scan = False
        self.end_of_det_stream = False
    
        context = zmq.Context()
        self.det_socket = context.socket(zmq.PULL)
        self.det_socket.connect(f'{det_host}:{det_port}')  ## ('tcp://b-daq-node-2:20001')
        self.pos_socket = context.socket(zmq.SUB)
        self.pos_socket.connect(f'{pos_host}:{pos_port}')  ## ("tcp://172.16.125.30:5556")# ("tcp://b-nanomax-controlroom-cc-3:5556")
        self.pos_socket.setsockopt(zmq.SUBSCRIBE, b"")  # subscribe to all topics
        self.relay_socket = context.socket(zmq.REP)
        self.relay_socket.bind(f'{relay_host}:{relay_port}')
        self.all_parts = []
        self.all_info_json = []
        self.all_info = []
        self.all_img = []
        self.all_msg = []
        self.Energy = None
        self.recieved_image_indices = []
        self.recieved_pos_indices = []
        self.latest_posimg_index_received = []
        self.nr_of_check_replies = 0

    def run(self):
        i = -1
        j = -1
        while self.running:
            try:
                try:
                    self.latest_posimg_index_received.append([self.latest_pos_index_received, self.latest_det_index_received, self.recieved_image_indices[-1]])
                except:
                    pass
                print('')
                print('receiving...')
                # find the first socket, this call blocks
                ready_sockets = zmq.select([self.det_socket, self.pos_socket, self.relay_socket], [], [], None)[
                    0]  # None makes the code wait here until there is something to read.. look at man select!
                print('** SELECT:')
                print(ready_sockets)

                # now read from the first ready socket
                ## ToDo: See if I could send reply directly under e.g. "if msg['status'] == 'started'" or if that would get stuck there then.

                if self.relay_socket in ready_sockets:
                    # Expects a request of the form: ['check/load', {'frame': int, '**kwargs': value}]
                    request = self.relay_socket.recv_json()
                    print(f'request = {request}')
                    if request[0] == 'check':
                        print('check')
                        if self.nr_of_check_replies == 0 and self.Energy != None:
                            print('inside REQ1') ### DEBUG
                            self.relay_socket.send_json(
                                    {'energy': self.Energy, 'self.latest_pos_index_received': self.latest_pos_index_received, 'self.latest_det_index_received': self.latest_det_index_received,
                                     'self.recieved_image_indices': self.recieved_image_indices})
                            self.nr_of_check_replies += 1
                        else:
                            print('inside REQ2')  ### DEBUG
                            # nr of images:
                            if len(self.recieved_image_indices) != self.latest_det_index_received + 1:  ## "+1" Because counting starts on 0
                                im_acc = len(self.recieved_image_indices)
                                print('Some diffraction pattern(s) got lost!!')  ## Maybe raise a warning?
                            else:
                                im_acc = self.latest_det_index_received + 1
                            # nr of positions:
                            frames_accessible_tot = min(im_acc,
                                                           self.latest_pos_index_received + 1)  # Just sending total nr of frames accessible, even if some of them have already been sent
                            print(f'sending [int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream] = {[int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream]}') ### DEBUG
                            self.relay_socket.send_json([int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream])
                            self.nr_of_check_replies += 1
                    elif request[0] == 'load':
                        print('load')
                        frame_nr = request[1]['frame']
                        print(f'frame_nr = {frame_nr},    type = {type(frame_nr)}')  ### DEBUG
                        # Take account for lost images and fix the indices
                        paired_pos_ind = np.array(self.recieved_pos_indices)[self.recieved_image_indices] ### IndexError: index 45 is out of bounds for axis 0 with size 4
                        print(f'paired_pos_ind = {paired_pos_ind},    type = {type(paired_pos_ind)}')  ### DEBUG
                        print(f'type(self.all_msg) = {type(self.all_msg)}')  ### DEBUG
                        self.all_msg_send = np.array(self.all_msg)[paired_pos_ind]
                        print(f'type(self.all_msg_send[frame_nr]) = {type(self.all_msg_send[frame_nr])}')  ### DEBUG
                        print(f'type(np.array(self.all_img)[frame_nr]) = {type(np.array(self.all_img)[frame_nr])}')  ### DEBUG
                        print(f'type(self.all_img) = {type(self.all_img)}')  ### DEBUG
                        print(f'type(self.all_img[frame_nr]) = {type(self.all_img[frame_nr])}')  ### DEBUG, TypeError: list indices must be integers or slices, not list
                        print(f'')  ### DEBUG
                        ## ToDo: Find the best method to send reply with
                        # self.relay_socket.send_json({'pos': self.all_msg_send[frame_nr], 'img': self.all_img[frame_nr]}) ### TypeError: list indices must be integers or slices, not list
                        self.relay_socket.send_json({'pos': self.all_msg_send[frame_nr], 'img': np.array(self.all_img)[frame_nr]}) ### TypeError: Object of type ndarray is not JSON serializable

                if self.det_socket in ready_sockets:
                    i += 1
                    print(f'**** EIGER: {time.strftime("%H:%M:%S", time.localtime())}')
                    parts = self.det_socket.recv_multipart(copy=False)
                    self.all_parts.append(parts)
                    ###info = json.loads(parts[0])##
                    ###print(info)##
                    ###header = json.loads(parts[0].bytes) # python dct
                    ###print(header)
                    info_json = parts[
                        0].bytes  ## b'{"filename":"/data/staff/nanomax/commissioning_2022-1/20220328/raw/sample/scan_000001_eiger.hdf5","htype":"header","msg_number":60162}'
                    self.all_info_json.append(info_json)
                    info = json.loads(info_json)  ## makes a dict out of info_json
                    self.all_info.append(info)
                    print('info: ', info)
                    if info['htype'] == 'header':
                        print('********************* EIGER STARTING')
                    elif info['htype'] == 'image':
                        print(f'Got diffraction pattern nr. {self.latest_det_index_received + 1}, decompressing...')
                        print(f'parts[1].buffer.nbytes = {parts[1].buffer.nbytes}')
                        # Temporary fix for dealing with bitshuffle weirdness
                        if det_host == 'tcp://b-daq-node-2':
                            ## This works when using modules at NanoMax, conda locally, but not with modules locally..
                            ## However conda accepts any starting piont of the buffer and therefore gives wrong result.
                            img = decompress_lz4(np.frombuffer(parts[1].buffer[12:], dtype=np.dtype('uint8')), info['shape'],
                                                 np.dtype(info['type']))  ## This is what's working when using modules at NanoMax, but not with modules locally..
                        else:
                            ## This doesn't work when using modules at NanoMax, but works on conda locally, and with modules locally..
                            img = decompress_lz4(np.frombuffer(parts[1].buffer, dtype=np.dtype('uint8')), info['shape'], np.dtype(info['type']))
                        self.all_img.append(img)
                        self.recieved_image_indices.append(info['frame'])
                        self.latest_det_index_received += 1
                    ##print('...done: ', type(img), img.shape, img.dtype)
                    elif info['htype'] == 'series_end':
                        self.end_of_det_stream = True
                        print('End of detector stream')

                if self.pos_socket in ready_sockets:
                    print(f'\t**** CONTRAST: {time.strftime("%H:%M:%S", time.localtime())}')
                    msg = self.pos_socket.recv_pyobj()
                    if msg['status'] == 'started':
                        j += 1
                        self.all_msg.append(msg)
                        print("\tmsg['status'] = motors started")
                        self.Energy = msg['snapshot']['energy']
                    elif msg['status'] == 'running':
                        j += 1
                        self.all_msg.append(msg)
                        self.latest_pos_index_received += 1
                        self.recieved_pos_indices.append(j)
                        print("\tmsg['status'] = motors running, position nr. %d recieved" % self.latest_pos_index_received)
                    elif msg['status'] == 'finished':
                        j += 1
                        self.all_msg.append(msg)
                        self.end_of_scan = True
                        print("\tmsg['status'] = motors finished")
                        print('\n\n')
                    else:
                        print('Message was not important')

                    for key, value in msg.items():
                        print(f'\t {str(key)} :\t {str(value)}')


                if self.end_of_scan and self.end_of_det_stream:
                    self.stop()

            except KeyboardInterrupt:
                self.stop()

            except Exception as err:
                print(err)
                self.stop()


    def stop(self):
   # if self.end_of_scan and self.end_of_det_stream:
        print(f'Last diffraction pattern and positions received at {time.strftime("%H:%M:%S", time.localtime())}, will close self.det_socket, self.pos_socket, and self.relay_socket!')
        self.det_socket.close()
        self.pos_socket.close()
        self.relay_socket.close()  # ToDo: Fix to end only after sending last frame
        self.running = False
        # return self.all_parts, self.all_info_json, self.all_info, self.all_img, self.all_msg, self.latest_posimg_index_received



if __name__ == "__main__":
    # info about which hosts and ports to use are in gitlab>streaming-receiver>detector-config.json
    det_host = 'tcp://0.0.0.0'  ## Local: 'tcp://0.0.0.0' # NanoMax Eiger4M: 'tcp://b-daq-node-2' , NanoMax Eiger1M: 'tcp://b-daq-node-2'
    det_port = '56789'  ## Local: '56789' # NanoMax Eiger4M: '20001' , NanoMax Eiger1M: '20007'
    pos_host = 'tcp://127.0.0.1'  ## Local: 'tcp://127.0.0.1'# NanoMax contrast: 'tcp://172.16.125.30'
    pos_port = '5556'  # NanoMax contrast: '5556'
    relay_host = 'tcp://127.0.0.1'  # Used for sending data to ptypy
    relay_port = '45678'
    RS = RelayServer(det_host=det_host, det_port=det_port, pos_host=pos_host, pos_port=pos_port, relay_host=relay_host, relay_port=relay_port)
    ### self.all_parts, self.all_info_json, self.all_info, self.all_img, self.all_msg, self.latest_posimg_index_received = RelayServer(det_host, det_port, pos_host, pos_port, relay_host, relay_port)
    RS.run()

    # pubout = self.runpub.communicate()[0].decode().split('\n')
    # pushout = self.runpush.communicate()[0].decode().split('\n')

"""
To do:
* Incorporate relays in contrasts recorders?
* Why are we using False in recv_multipart(copy=False) ??
* Make a class of it to properly close sockets, otherwise I might get just every other message
	"check if you can do a with socket open" -- Maik
* Mail zdenek about difference between bitshuffles
"""

"""
# Example if we'd expect to have 9 frames: 


self.recieved_image_indices = [1, 2, 3, 4, 5, 6, 7, 8] # .append(info['frame'])





self.all_msg = [{'status': 'heartbeat'},
			{'scannr': 29, 'status': 'started', 'path': '...', ...},
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			{'status': 'heartbeat'},
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			OrderedDict([ (), (), (), ...]),
			{'scannr': 29, 'status': 'finished', 'path': '...', ...}
			]


self.recieved_pos_indices = [2, 3, 4, 5, 6, 7, 9, 10, 11] # .append(j)
ALLMSG = self.all_msg[self.recieved_pos_indices]

# sent frame order: 		0, 1, 2, 3, 4, 5, 6, 7, 8
available_frames = {'pos': [2, 3, 4, 5, 6, 7, 9, 10,11],
					'img': [X, 1, 2, 3, 4, 5, 6, 7, 8]}


"""

##############################################################################################
#### check() function based on nanomax_streming.py
##############################################################################################

"""
self.end_of_scan, self.end_of_det_stream = False, False

	# get all frames from the main socket
	while True:
	    try:
		msg = self.socket.recv_pyobj(flags=zmq.NOBLOCK)  ## NOBLOCK returns None if a message is not ready
		logger.info('######## Received a message')  ##
		##headers = ('path' in msg.keys())
		##emptymsg = ('heartbeat' in msg.values()) # just a message from contrast to keep connection alive
		######if 'running' in msg.values():  # if zmq did not send a path: then save this message
		if msg['status'] == 'started':
		    self.meta.energy = np.float64([msg['snapshot']['energy']]) * 1e-3 ## Read energy from beamline snapshot
		    logger.info('############ RecorderHeader received; SCAN STARTING!')  ##
		elif msg['status'] == 'running':
		    self.latest_pos_index_received += 1
		    self.incoming[self.latest_pos_index_received] = msg
		    logger.info('############ Frame nr. %d received' % self.latest_pos_index_received)  ##
		    break ## include this break if you want to start iterations befora all frames have been acquired
		elif msg['status'] == 'finished': ## 'msgEOS':  # self.EOS:
		    self.end_of_scan = True
		    logger.info('############ RecorderFooter received; END OF SCAN!')  ##
		    break
		else:
		    logger.info('############ Message was not important')  ##
	    except zmq.ZMQError:
		logger.info('######## Waiting for messages')  #w2#
		# no more data available - working around bug in ptypy here
		if self.latest_pos_index_received < self.info.min_frames * parallel.size:
		    logger.info('############ self.latest_pos_index_received = %u , self.info.min_frames = %d , parallel.size = %d' % (self.latest_pos_index_received, self.info.min_frames, parallel.size))  ##
		    logger.info('############ Not enough frames received, have %u frames, waiting...' % (self.latest_pos_index_received + 1))
		    time.sleep(1)
		else:
		    logger.info('############ Will process gathered data')  ##
		    break

	# get all frames from the detector socket
	while self.stream_images:
	    try:
		parts = self.det_socket.recv_multipart(flags=zmq.NOBLOCK)
		info = json.loads(parts[0])
		shape = info['shape']
		dtype = np.dtype(info['type'])
		img = decompress(parts[1], shape, dtype)
		self.latest_det_index_received += 1
		self.incoming_det[self.latest_det_index_received] = img

	    except zmq.ZMQError:
		# no more data available - working around bug in ptypy here
		if self.latest_det_index_received < self.info.min_frames * parallel.size:
		    logger.info('have %u detector frames, waiting...' % (self.latest_det_index_received + 1))
		    time.sleep(.5)
		else:
		    break

"""

# %%

