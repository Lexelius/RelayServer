import json
from typing import List, Any

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
import os
from ptypy import utils as u
import inspect
import h5py
import copy


class RelayServer(object):
    recieved_image_indices: List[Any]

    def __init__(self):
        """ Declares necessary variables. """
        self.t0 = time.time()
        # if simulate == True:
        #         self.runpub = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Motor_streamer.py'],
        #                                   stdout=subprocess.PIPE,
        #                                   stderr=subprocess.STDOUT)
        #         self.runpush = subprocess.Popen([sys.executable, '/home/reblex/RelayServer/Simulators/Detector_streamer.py'],
        #                                    stdout=subprocess.PIPE,
        #                                    stderr=subprocess.STDOUT)

        # Initialize parameters
        self.RS_path = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))
        self.running = None
        self.latest_pos_index_received = -1
        self.latest_det_index_received = -1  # Counting every received image, assumes they come in the correct order and that no images are lost
        self.end_of_scan = False
        self.end_of_det_stream = False

        self.all_img = {}
        self.all_msg = {}
        self.Energy = None
        self.recieved_image_indices = []  # List of received frame indices, read from detector messages
        self.recieved_pos_indices = []
        self.latest_posimg_index_received = []
        self.nr_of_check_replies = 0
        self.energy_replied = False
        self.load_replies = 0
        self.init_params = {}
        self.do_crop = None
        self.do_pos_aver = None
        self.center = None
        self.newcenter = None
        self.sendimg = []
        self.auto_rerun = False  # Start listening for new data again after the scan finishes
        self.motors_started = False
        self.detector_started = False
        self.dettottime = 0  ### DEBUG
        self.postottime = 0  ### DEBUG
        self.reltottime = 0  ### DEBUG
        self.reltottimewall = 0  ### DEBUG
        self.zmqtottime = 0  ### DEBUG
        print(self.__dict__)

    def connect(self, detector_address, motors_address, relay_address, simulate):
        """
        Initiates the connections and binding of the sockets.
        Starts simulating an ongoing experiment in new processes
        if 'simulate' is set to True.

        Parameters
        ----------
        detector_address : str
        motors_address : str
        relay_address : str
        simulate : bool

        """
        print(f'Starting Relay server, reading from detector address {detector_address} and positions address {motors_address}')
        context = zmq.Context()
        self.det_socket = context.socket(zmq.PULL)
        self.det_socket.connect(detector_address)  ## ('tcp://b-daq-node-2:20001')

        self.pos_socket = context.socket(zmq.SUB)
        self.pos_socket.connect(motors_address)  ## ("tcp://172.16.125.30:5556")# ("tcp://b-nanomax-controlroom-cc-3:5556")
        self.pos_socket.setsockopt(zmq.SUBSCRIBE, b"")  # subscribe to all topics

        self.relay_socket = context.socket(zmq.REP)
        self.relay_socket.bind(relay_address)

        # Start simulating an ongoing experiment
        if simulate:
            self.runpub = subprocess.Popen([sys.executable, self.RS_path + '/Simulators/Motor_streamer.py'],
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT)
            self.runpush = subprocess.Popen([sys.executable, self.RS_path + '/Simulators/Detector_streamer.py'],
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT)

        self.decomp_from_byte12 = detector_address.rsplit(':', 1)[0] == 'tcp://p-nanomax-eiger-1m-daq.maxiv.lu.se'  # 'tcp://p-daq-cn-2'  ##'tcp://b-daq-node-2' ## used to determine how to decompress images

    def run(self):
        # ToDO: Add some assertion/check that sockets have been connected before continuing from here.
        self.i = -1
        self.j = -1
        self.running = True
        while self.running:
            try:
                # !#try:
                # !#self.latest_posimg_index_received.append([self.latest_pos_index_received, self.latest_det_index_received, self.recieved_image_indices[-1]])
                # !#except:
                # !#pass
                print('')
                print('receiving...')
                print('Received %d positions and %d images\r' % (self.latest_pos_index_received + 1, self.latest_det_index_received + 1), end='')
                # Find the first socket
                t0 = time.perf_counter()  ### DEBUG
                ready_sockets = zmq.select([self.det_socket, self.pos_socket, self.relay_socket], [], [], None)[
                    0]  # None makes the code wait here until there is something to read.. look at man select!
                t1 = time.perf_counter()  ### DEBUG
                self.zmqtottime += t1 - t0  ### DEBUG
                print(('Time spent in zmq.select() = %f, accumulated time = %f' % ((t1 - t0), self.zmqtottime)))  ### DEBUG
                print('** SELECT:')
                print(ready_sockets)

                # now read from the first ready socket
                ## ToDo: See if I could send reply directly under e.g. "if msg['status'] == 'started'" or if that would get stuck there then.

                if self.det_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    self.det_action()
                    t1 = time.perf_counter()  ### DEBUG
                    self.dettottime += t1 - t0  ### DEBUG
                    print(('Time spent in det_action() = %f, accumulated time = %f' % ((t1 - t0), self.dettottime)))  ### DEBUG

                if self.pos_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    self.pos_action()
                    t1 = time.perf_counter()  ### DEBUG
                    self.postottime += t1 - t0  ### DEBUG
                    print(('Time spent in pos_action() = %f, accumulated time = %f' % ((t1 - t0), self.postottime)))  ### DEBUG

                if self.relay_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    t00 = time.time()  ### DEBUG
                    # Expects a request of the form: ['check/load', {'frame': int, '**kwargs': value}]
                    print(f'**** RELAY: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
                    request = self.relay_socket.recv_json()
                    print(f'request = {request}')
                    ### ToDo: Change "check_energy"-request to "initialize"-request and implement params for cropping, rebinning, background subtract
                    # !#
                    if request[0] == 'preprocess':
                        self.init_params = request[1]
                        self.relay_socket.send_json(['Preprocess message received'])
                        self.do_crop = 'shape' in self.init_params.keys()
                        self.do_rebin = 'rebin' in self.init_params.keys()
                        self.do_pos_aver = 'average_x_at_RS' in self.init_params.keys()
                        self.do_masking = 'maskfile' in self.init_params.keys()
                        if self.do_masking:
                            maskfile = self.init_params['maskfile']
                            with h5py.File(maskfile, 'r') as f:
                                self.mask = f['mask'][:]

                        # !# self.do_rebin = ...

                    # !#
                    if request[0] == 'check_energy':
                        if not self.energy_replied and self.Energy != None:
                            self.relay_socket.send_json({'energy': float(self.Energy)})
                            self.energy_replied = True
                            self.nr_of_check_replies += 1
                            print(f'Replied that energy = {float(self.Energy)}')
                        else:
                            self.relay_socket.send_json({'energy': False})
                    elif request[0] == 'check':
                        print('check')
                        print('inside REQ2')  ### DEBUG
                        frames_accessible = list(set(list(self.all_msg.keys())).intersection(list(self.all_img.keys())))
                        frames_accessible_tot = len(frames_accessible)  # !# ToDo: LS MUST BE UPDATED: Now sending nr of NEW frames accessible!!!
                        print(f'sending [int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream] = {[int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream]}')  ### DEBUG
                        self.relay_socket.send_json([int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream])
                        self.nr_of_check_replies += 1
                    elif request[0] == 'load':
                        print('load')
                        self.frame_nr = request[1]['frame']
                        # Get the correct indices corresponding to the requested frame_nr
                        sendmsg = [self.all_msg.pop(key) for key in self.frame_nr]
                        # !###sendimg = np.array([self.crop(self.all_img.get(key)) for key in self.frame_nr]) #!# CHANGE get TO POP!
                        sendimg = np.array([self.all_img.pop(key) for key in self.frame_nr])  # !# CHANGE get TO POP! BUT ADD DEBUG/TEST OPTION WHICH DOES USE GET!!
                        if self.do_crop:
                            sendimg, self.newcenter, self.padmask = self.crop(sendimg)
                            if self.load_replies == 0:
                                sendmsg[0]['new_center'] = np.array(self.newcenter)
                                if self.do_masking:
                                    self.mask, newcenter, padmask = self.crop(self.mask)

                        if self.do_rebin:
                            try:
                                weight = np.ones_like(sendimg)
                                weight[np.where(sendimg == 2 ** 32 - 1)] = 0
                                weight = u.rebin_2d(weight, self.init_params['rebin'])
                                sendimg = u.rebin_2d(sendimg, self.init_params['rebin'])
                                if self.load_replies == 0 and self.do_masking:
                                    self.mask = u.rebin_2d(self.mask, self.init_params['rebin'])
                                    sendimg = np.array([sendimg, weight, self.mask])
                                else:
                                    sendimg = np.array([sendimg, weight])
                                sendmsg[0]['RS_rebinned'] = True
                                if self.newcenter is not None:
                                    sendmsg[0]['new_center'] = np.array(self.newcenter) / float(self.init_params['rebin'])
                            except:
                                print('Warning: could not rebin, leaving this task to PtyPy instead.')
                                sendmsg[0]['RS_rebinned'] = False

                        print('DEBUG 1')
                        self.sendimg.append(sendimg)  # !# DEBUG, remove line later
                        print('DEBUG 2')
                        sendmsg[0]['dtype'] = sendimg[0].dtype  # Used for decompressing image in LS
                        print('DEBUG 3')
                        sendmsg[0]['shape'] = sendimg.shape  # Used for decompressing image in LS
                        print('DEBUG 4')
                        self.relay_socket.send_pyobj(sendmsg, flags=zmq.SNDMORE)
                        print(f'DEBUG 5, ')
                        self.relay_socket.send(compress_lz4(sendimg), copy=True)
                        print('DEBUG 6')
                        self.load_replies += 1
                    elif request[0] == 'stop':
                        ##ToDo change this method so it closes the socket if RS has sent everything instead of waiting for a request from the LS side!
                        self.relay_socket.send_json(['closing connection to relay_socket'])
                        self.stop_outstream()
                    t1 = time.perf_counter()  ### DEBUG
                    t11 = time.time()  ### DEBUG
                    self.reltottime += t1 - t0  ### DEBUG
                    self.reltottimewall += t11 - t00  ### DEBUG
                    print(('Time spent in if-relay = %f, accumulated time = %f, accululated walltime = %f' % ((t1 - t0), self.reltottime, self.reltottimewall)))  ### DEBUG

                # print('Received %d positions and %d images\r' % (self.latest_pos_index_received+1, self.latest_det_index_received+1), end='', flush=True)

                if self.end_of_scan and self.end_of_det_stream:  # and not (self.det_socket.closed and self.pos_socket.closed):
                    self.stop()

            # To make sure sockets gets closed
            except KeyboardInterrupt:
                self.stop()
                self.stop_outstream()

            except Exception as err:
                print('Error: ', err)
                self.stop()
                self.stop_outstream()

    def det_action(self):
        self.i += 1
        print(f'**** DETECTOR: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
        parts = self.det_socket.recv_multipart(copy=False)
        info = json.loads(parts[0].bytes)  ## makes a dict out of info_json
        ### self.all_info.append(info)
        print('info: ', info)

        if not self.motors_started and not self.detector_started:
            if 'filename' in info.keys() and info['filename'] != '':
                print('This should be the initial message of a scan')  ### DEBUG
                print('********************* DETECTOR STARTING')
                self.detector_started = True
            else:
                print('Image or message from Live-mode or already started scan')  ### DEBUG
        else:
            # if info['htype'] == 'header':
            #     print('********************* DETECTOR STARTING')
            if info['htype'] == 'image':
                print(f'parts[1].buffer.nbytes = {parts[1].buffer.nbytes}')
                ##################################
                ## FOR NANOMAX
                ##################################
                # Temporary fix for dealing with bitshuffle weirdness
                if info['compression'] == 'bslz4':
                    if self.decomp_from_byte12:
                        ## This works when using modules at NanoMax, conda locally, but not with modules locally..
                        ## However conda accepts any starting piont of the buffer and therefore gives wrong result.
                        img = decompress_lz4(np.frombuffer(parts[1].buffer[12:], dtype=np.dtype('uint8')), info['shape'],
                                             np.dtype(info['type']))  ## This is what's working when using modules at NanoMax, but not with modules locally..
                    else:
                        ## This doesn't work when using modules at NanoMax, but works on conda locally, and with modules locally..
                        img = decompress_lz4(np.frombuffer(parts[1].buffer, dtype=np.dtype('uint8')), info['shape'], np.dtype(info['type']))
                elif info['compression'] == 'none':
                    ##################################
                    ## FOR SOFTIMAX (no compression)
                    ##################################
                    img = np.frombuffer(parts[1].buffer, dtype=np.dtype(info['type'])).reshape(info['shape'])
                    ##################################
                else:
                    print(f"Unknown compression type: {info['compression']}")

                # %#self.all_img.append(img)
                self.latest_det_index_received += 1
                self.all_img[self.latest_det_index_received] = img  ##[info['frame']] = img ##
                self.recieved_image_indices.append(info['frame'])
            elif info['htype'] == 'series_end':
                self.end_of_det_stream = True
                print('End of detector stream')

    def pos_action(self):
        print(f'\t**** CONTRAST: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
        msg = self.pos_socket.recv_pyobj()
        if not self.motors_started:
            if msg['status'] == 'started':
                print("\tmsg['status'] = motors started")
                if 'energy' in msg['snapshot'].keys():
                    self.Energy = msg['snapshot']['energy']  ### NANOMAX: key is 'energy'
                elif 'beamline_energy' in msg['snapshot'].keys():
                    self.Energy = msg['snapshot']['beamline_energy']  ### SOFTIMAX: key is 'beamline_energy'
                self.motors_started = True
            else:
                print('Message was not important')
        else:
            if msg['status'] == 'running':
                self.j += 1
                self.latest_pos_index_received += 1
                self.all_msg[self.j] = msg
                # msgs = self.divide_burst_msg(msg)
                # # %#self.all_msg.append(msg)
                # for k in range(self.j, self.j + len(msgs)):
                #     self.all_msg[k] = msg  # %#
                # self.j = len(self.all_msg)
            elif msg['status'] == 'finished':
                self.end_of_scan = True
                print("\tmsg['status'] = motors finished")
                print('\n\n')
            elif msg['status'] == 'interrupted':
                self.end_of_scan = True
                print("\tmsg['status'] = interrupted")
                print('\n\n')
            else:
                print('Message was not important')

        for key, value in msg.items():
            print(f'\t {str(key + ":").ljust(15)} {str(value)}')

    def divide_burst_msg(self, msg_burst):
        xMotor = 'panda0/INENC1.VAL_Value'  ### HARDCODING
        xMotorKeys = xMotor.split('/')  ### HARDCODING
        n_burst = msg_burst[xMotorKeys[0]][xMotorKeys[1]].__len__()
        nr = np.linspace(0, n_burst - 1, n_burst, dtype=int)
        msgs_divided = list(map(lambda i: self.divide_msgs(copy.deepcopy(msg_burst), i), nr))
        return msgs_divided

    def walk_dict(self, dct):
        """
        A recursive version of dict.items(), which yields
        (containing-dict, key, val).
        """
        for k, v in dct.items():
            yield dct, k, v
            if isinstance(v, dict):
                for d_, k_, v_ in self.walk_dict(v):
                    yield d_, k_, v_

    # Extracts a single message from the dict-array of messages
    def divide_msgs(self, dct, i):
        for d, k, v in self.walk_dict(dct):
            if isinstance(v, np.ndarray):
                if k == 'thumbs:':
                    d[k] = v[i].base  # converts b'None' into None
                else:
                    d[k] = v[i:i + 1]
        # dct['status'] = 'running'
        return dct

    # Close sockets
    def stop(self):
        # if self.end_of_scan and self.end_of_det_stream:
        print(f'Closing self.det_socket, self.pos_socket! at time {time.strftime("%H:%M:%S", time.localtime())}')
        print('Received %d pos, %d img' % (self.latest_pos_index_received + 1, self.latest_det_index_received + 1))
        self.det_socket.close()
        self.pos_socket.close()

    def stop_outstream(self):
        print(f'Closing the relay_socket at {time.strftime("%H:%M:%S", time.localtime())}! {time.time() - self.t0:.04f} seconds')
        self.relay_socket.close()
        self.running = False
        if self.auto_rerun:  ## MAYBE MOVE THIS SOMEWHERE ELSE SO IT DOESN'T RESTART WHEN KeyboardInterrupt!!
            self.__init__
            self.run()

    def crop(self, diff, get_weights=True):
        """
        Crops a single diffraction pattern of type ndarray.

        Padding to make sure that diffraction center is in the image center
        is yet not implemented, but performed in ptypy.

        ToDo: Check if ptypy incorporates mask when calculating center

        Parameters
        ----------
        diff
        get_weights

        Returns
        -------

        """

        # Only allow square slices in data
        self.init_params['shape'] = np.min(self.init_params['shape'])

        # a: desired crop shape, b: resulting crop shape, c: center , d: diff shape

        if self.center is None:
            ##if 'center' in self.init_params.keys() and not isinstance(self.init_params['center'], str):
            if 'center' in self.init_params.keys() and isinstance(self.init_params['center'], (list, tuple)):
                c_d = np.array(self.init_params['center'])  ## cy, cx : row, col  ### [751, 343]
            else:
                c_d = np.rint(self.find_center('all', 'auto')).astype(int)
            self.center = c_d
        else:
            c_d = self.center

        # Find limits for cropping
        d_sh = diff.shape[-2:]  # (rows, cols) (yspan, xspan)
        a_sh = int(self.init_params['shape'])

        a_lowlims = c_d - a_sh // 2  # !##  array([367, -41])
        a_highlims = c_d + (a_sh + 1) // 2  # (a_sh + 1) to enable uneven nr of pixels.

        # Set the limits within the boundaries of diffraction pattern
        b_lowlims = np.max((a_lowlims, np.zeros(2, dtype='int')), axis=0)
        b_highlims = np.min((a_highlims, np.array(d_sh)), axis=0)
        lims = np.array([b_lowlims, b_highlims])

        ##diff_crop = diff[b_lowlims[0]:b_highlims[0], b_lowlims[1]:b_highlims[1]]
        diff_crop = np.ascontiguousarray(np.split(np.split(diff, lims[:, 0], axis=-2)[1], lims[:, 1], axis=-1)[1])  # using ascontiguousarray automatically means that you'll make a copy so the array can be stored contiguously.

        c_a = np.array([a_sh // 2, a_sh // 2])
        c_b = tuple(c_d - b_lowlims)  # center w.r.t cropped diffraction pattern

        # Adding padding
        padlims = np.array([b_lowlims - a_lowlims, a_highlims - b_highlims])
        padwidth = np.zeros((len(diff.shape), 2), dtype='int64')  # Used to get the correct shape for padwidth in np.pad
        padwidth[-2:, :] = np.array([(padlims[:, 0], padlims[:, 1])])
        diff_croppad = np.pad(diff_crop, padwidth)
        ####### TODO: INCLUDE PADDING INTO THE WEIGHTS!!!! + update LS now that cropping always works (no need for checking this)
        if get_weights:
            w = np.ones_like(diff_crop)
            w[np.where(diff_crop == 2 ** 32 - 1)] = 0
            w = np.pad(w, padwidth)

        return diff_croppad, c_a, padwidth
        # !#return diff_crop, c_b

    def find_center(self, diff, mask=None):
        """
        Finds the center of mass in the diffraction patterns.
        Center is calculated using all diffraction patterns at the same time,
        meaning that if one diffraction pattern would have half of the intensity
        of a second diffraction pattern, then it will also contribute half as
        much, compared to the second one.
        ToDo: make all contribute equally

        Parameters
        ----------
        diff : ##dict of 2D ndarrays
        mask : 2D ndarray

        Returns
        -------

        """
        if isinstance(diff, str):
            diff = self.all_img
        diff = np.array([diff[key] for key in diff.keys()])
        axes = tuple(range(1, diff.ndim + 1))

        if mask == 'auto':
            mask = np.ones_like(diff[0])
            mask[np.where(diff[0] == 2 ** 32 - 1)] = 0

        if mask is None:
            return (np.sum(diff * np.indices(diff.shape), axis=axes, dtype=float) / np.sum(diff, dtype=float))[-2:]
        else:
            return (np.sum(diff * mask * np.indices(diff.shape), axis=axes, dtype=float) / np.sum(diff * mask, dtype=float))[-2:]

    def average_positions(self, sendmessage):
        """
        Used when there is 2 x- and y positions per diffraction image.
        Averages two positions.

        Currently hard coding the depth of these positions to be 2 levels down!
        Started writing this function assuming that the 2 positions are in 2 separate messages!!!
        :param sendmessage:
        :return:
        """
        x_keys = self.init_params.average_x_at_RS.split('/')
        y_keys = self.init_params.average_y_at_RS.split('/')
        raw_x = np.array([sendmessage[pos][x_keys[0]][x_keys[1]] for pos in sendmessage.keys()])
        raw_y = np.array([sendmessage[pos][y_keys[0]][x_keys[1]] for pos in sendmessage.keys()])

        raw_x_t0 = raw_x[::2]  # position at the start of frame aquisition
        raw_y_t0 = raw_y[::2]  # position at the start of frame aquisition
        raw_x_t1 = raw_x[1::2]  # position at the end of frame aquisition
        raw_y_t1 = raw_y[1::2]  # position at the end of frame aquisition
        x_aver = (raw_x_t0 + raw_x_t1) / 2
        y_aver = (raw_y_t0 + raw_y_t1) / 2

        ## ToDo: Fix this part when you know how the positions
        sendmessage_averpos = 0
        return sendmessage_averpos


# In progress:
# def rebin(self, diff, return_weights=True):
#     w = np.ones_like(diff_crop)
#     ## padwidth: (padrows_start, padrows_end), (padcols_start, padcols_ebd)
#     w[:, :, ()]
#     np.pad(w, padwidth//2)
#
#     weight = np.ones_like(sendimg)
#     weight[np.where(sendimg == 2 ** 32 - 1)] = 0
#     weight = u.rebin_2d(weight, self.init_params['rebin'])
#
#     sendimg = u.rebin_2d(sendimg, self.init_params['rebin'])
#     sendimg = np.array([sendimg, weight])
#     sendmsg[0]['RS_rebinned'] = True
#     if return_weights:
#
#         return diff
#     else:
#         return diff


def launch(RS=None):
    if RS is None:
        RS = RelayServer()
    # info about which hosts and ports to use are in gitlab>streaming-receiver>detector-config.json
    #     https://gitlab.maxiv.lu.se/scisw/detectors/streaming-receiver-cpp
    known_sources = {'Simulator':       {'det_adr': 'tcp://0.0.0.0:56789', 'pos_adr': 'tcp://127.0.0.1:5556'},
                     # 'NanoMAX_eiger1M': {'det_adr': 'tcp://b-daq-node-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     #                   'NanoMAX_eiger1M': {'det_adr': 'tcp://p-daq-cn-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     'NanoMAX_eiger4M': {'det_adr': 'tcp://b-daq-node-2:20001', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     'NanoMAX_eiger1M': {'det_adr': 'tcp://p-nanomax-eiger-1m-daq.maxiv.lu.se:5556', 'pos_adr': 'tcp://172.16.125.30:5556'}, 'NanoMAX_eiger4M': {'det_adr': 'tcp://b-daq-node-2:20001', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     ## pos_adr for NanoMAX can also be: 'tcp://b-nanomax-controlroom-cc-3:5556'
                     # need to login to blue network - SOFTIMAX to connect to det and pos
                     'SoftiMAX_andor':  {'det_adr': 'tcp://p-fanout-softimax-xzyla-andor3:10000', 'pos_adr': 'tcp://172.16.205.5:5556'}  # det_adr: tcp://b-softimax-cams-0:20007, pos_adr: b-softimax-cc-0
                     }
    src = known_sources['Simulator']
    relay_adr = 'tcp://127.0.0.1:45678'

    # RS = RelayServer(detector_address=src['det_adr'], motors_address=src['pos_adr'], relay_address=relay_adr, simulate=True)
    ## RS = RelayServer()
    RS.connect(detector_address=src['det_adr'], motors_address=src['pos_adr'], relay_address=relay_adr, simulate=True)
    RS.run()

    # pubout = RS.runpub.communicate()[0].decode().split('\n')
    # pushout = RS.runpush.communicate()[0].decode().split('\n')
    return known_sources, src, relay_adr, RS


if __name__ == "__main__":
    known_sources, src, relay_adr, RS = launch()
    # pubout = RS.runpub.communicate()[0].decode().split('\n')
    # pushout = RS.runpush.communicate()[0].decode().split('\n')
    # print(pushout[:10])
    # print(pubout[:10])
    # print(pushout[-10:])
    # print(pubout[-10:])