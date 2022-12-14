import json
import zmq
from bitshuffle import decompress_lz4
from bitshuffle import compress_lz4
import numpy as np
import select
import time
import bitshuffle
import numpy as np
import struct

import subprocess
import sys


class RelayServer(object):

    def __init__(self, detector_address, motors_address, relay_address, simulate):
        print('start')
        self.t0 = time.time()
        print(f'Starting Relay server, reading from detector address {detector_address} and positions address {motors_address}')
        if simulate == True:
                self.runpub = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Motor_streamer.py'],
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT)
                self.runpush = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Detector_streamer.py'],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT)
    
        context = zmq.Context()
        self.det_socket = context.socket(zmq.PULL)
        self.det_socket.connect(detector_address)  ## ('tcp://b-daq-node-2:20001')

        self.pos_socket = context.socket(zmq.SUB)
        self.pos_socket.connect(motors_address)  ## ("tcp://172.16.125.30:5556")# ("tcp://b-nanomax-controlroom-cc-3:5556")
        self.pos_socket.setsockopt(zmq.SUBSCRIBE, b"")  # subscribe to all topics

        self.relay_socket = context.socket(zmq.REP)
        self.relay_socket.bind(relay_address)

        # Initialize parameters
        self.running = True
        self.latest_pos_index_received = -1  ## ToDo: check if these 2 should be in the beginning of check() instead
        self.latest_det_index_received = -1
        self.end_of_scan = False
        self.end_of_det_stream = False
        self.decomp_from_byte12 = detector_address.rsplit(':', 1)[0] == 'tcp://p-daq-cn-2'##'tcp://b-daq-node-2' ## used to determine how to decompress images
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
        self.energy_replied = False

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
                # Find the first socket
                ready_sockets = zmq.select([self.det_socket, self.pos_socket, self.relay_socket], [], [], None)[
                    0]  # None makes the code wait here until there is something to read.. look at man select!
                print('** SELECT:')
                print(ready_sockets)

                # now read from the first ready socket
                ## ToDo: See if I could send reply directly under e.g. "if msg['status'] == 'started'" or if that would get stuck there then.

                if self.det_socket in ready_sockets:
                    i += 1
                    print(f'**** EIGER: {time.strftime("%H:%M:%S", time.localtime())}, {time.time()-self.t0} seconds')
                    parts = self.det_socket.recv_multipart(copy=False)
                    self.all_parts.append(parts)
                    ###info = json.loads(parts[0])##
                    ###print(info)##
                    ###header = json.loads(parts[0].bytes) # python dct
                    ###print(header)
                    info_json = parts[0].bytes
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
                        if self.decomp_from_byte12:
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
                    elif info['htype'] == 'series_end':
                        self.end_of_det_stream = True
                        print('End of detector stream')

                if self.pos_socket in ready_sockets:
                    print(f'\t**** CONTRAST: {time.strftime("%H:%M:%S", time.localtime())}, {time.time()-self.t0} seconds')
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

                if self.relay_socket in ready_sockets:
                    # Expects a request of the form: ['check/load', {'frame': int, '**kwargs': value}]
                    print(f'**** RELAY: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
                    request = self.relay_socket.recv_json()
                    print(f'request = {request}')
                    if request[0] == 'check_energy':
                        print('check_energy')
                        if not self.energy_replied and self.Energy != None:
                            self.relay_socket.send_json(
                                    {'energy': float(self.Energy), 'self.latest_pos_index_received': int(self.latest_pos_index_received),
                                     'self.latest_det_index_received': int(self.latest_det_index_received)})
                            self.energy_replied = True
                            self.nr_of_check_replies += 1
                        else:
                            self.relay_socket.send_json({'energy': False})
                    elif request[0] == 'check':
                        print('check')
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
                        self.frame_nr = request[1]['frame']
                        # Get the correct indices corresponding to the requested frame_nr
                        frame_msg_ind = np.array(self.recieved_pos_indices)[self.frame_nr]
                        frame_img_ind = np.array(self.recieved_image_indices)[self.frame_nr]
                        sendmsg = np.array(self.all_msg)[frame_msg_ind]
                        sendimg = np.array(self.all_img)[frame_img_ind]
                        sendmsg[0]['dtype'] = sendimg[0].dtype  # Used for decompressing image in LS
                        sendmsg[0]['shape'] = sendimg.shape  # Used for decompressing image in LS
                        self.relay_socket.send_pyobj(sendmsg, flags=zmq.SNDMORE)
                        self.relay_socket.send(compress_lz4(sendimg), copy=True)
                    elif request[0] == 'stop':
                        self.relay_socket.send_json(['closing connection to relay_socket'])
                        self.stop_outstream()


                if self.end_of_scan and self.end_of_det_stream:
                    self.stop()

            # To make sure sockets gets closed
            except KeyboardInterrupt:
                self.stop()
                self.stop_outstream()

            except Exception as err:
                print('Error: ', err)
                self.stop()
                self.stop_outstream()


    # Close sockets
    def stop(self):
   # if self.end_of_scan and self.end_of_det_stream:
        print(f'Closing self.det_socket, self.pos_socket! at time {time.strftime("%H:%M:%S", time.localtime())}')
        self.det_socket.close()
        self.pos_socket.close()
        print(f'self.recieved_pos_indices[-1] = {self.recieved_pos_indices[-1]}, self.recieved_image_indices[-1] = {self.recieved_image_indices[-1]}')


    def stop_outstream(self):
        print(f'Closing the relay_socket at {time.strftime("%H:%M:%S", time.localtime())}! {time.time()-self.t0} seconds')
        self.relay_socket.close()
        self.running = False


if __name__ == "__main__":
    # info about which hosts and ports to use are in gitlab>streaming-receiver>detector-config.json
    known_sources = {'Simulator': {'det_adr': 'tcp://0.0.0.0:56789', 'pos_adr': 'tcp://127.0.0.1:5556'},
                   #'NanoMAX_eiger1M': {'det_adr': 'tcp://b-daq-node-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                   'NanoMAX_eiger1M': {'det_adr': 'tcp://p-daq-cn-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                   'NanoMAX_eiger4M': {'det_adr': 'tcp://b-daq-node-2:20001', 'pos_adr': 'tcp://172.16.125.30:5556'}
                    ## pos_adr for NanoMAX can also be: 'tcp://b-nanomax-controlroom-cc-3:5556'
                   }
    src = known_sources['Simulator']
    relay_adr = 'tcp://127.0.0.1:45678'

    RS = RelayServer(detector_address=src['det_adr'], motors_address=src['pos_adr'], relay_address=relay_adr, simulate=True)
    RS.run()

    #pubout = RS.runpub.communicate()[0].decode().split('\n')
    #pushout = RS.runpush.communicate()[0].decode().split('\n')

