"""
A server that realys and preprocesses ptychographic data streamed via ZMQ.
Is expected to be run together with PtyPy, using the subclass LiveScan in the
livescan_maxiv branch which can be found at:
https://github.com/ptycho/ptypy/blob/livescan_maxiv/ptypy/experiment/livescan.py

The RelayServer listens to three ZMQ sockets, det_socket, pos_socket, and relay_socket.
The addresses for these sockets are specified in the "launch()" function.
To add new addresses from where the diffraction patterns and positions are streamed from,
you can add an entry in the dictionary "known_sources" in "launch()". The first entry in this dictionary
is "Simulator" and contains the addresses specified in the simulator scripts (Detector_streamer.py
and Motor_streamer.py).
There is no need to change these addresses, nor the "relay_adr", but if you do,
make sure to also change to the same address in the corresponding sender script
(for relay_adr this would be in the parameters p.scans.scan00.data.relay_host and
p.scans.scan00.data.relay_port of the reconstruction script)

You can then quickly switch between the sources which the RelayServer should listen to in
the parameter "src" in the "launch()" function.

Example of how to run the RelayServer from the terminal with the simulator:

$ input='{"sleeptime": "0.7125", "scan_file": "/your/path/scan_000026_eiger1m.hdf5", "path_to_data": "/entry/instrument/Eiger/data", "detector": "eiger1m"}'
$ python /home/reblex/RelayServer/relayserver.py "${input}"

where the input parameters are:
sleeptime: how much delay in seconds the streamers should pause in between sending each data message.
    For realistic conditions, a guideline could be around the average between the recorded time stamps
    in the h5 file. e.g.
        t = ptypy.io.h5read("/your/path/0001_setup/000026.h5", "entry/measurement/dt")["entry/measurement/dt"]
        sleeptime = np.mean(t[1:] - t[:-1])
scan_file: Absolute path to the raw data file containing the diffraction patterns.
path_to_data: The h5 key-path to where the diffraction patterns are stored in the scan_file.
detector: Name of the detector. Used to delete the links to the diffraction patterns when
    Motor_streamer.py divides the data points from the h5-file.
    >>>
"""
import json
from typing import List, Any
import zmq
from bitshuffle import decompress_lz4
from bitshuffle import compress_lz4
import time
import numpy as np
import subprocess
import sys
import os
from ptypy import utils as u
import inspect
import h5py
import copy
import logging
"""
Changes (2025/01/09):
* changed names of streames processes to something more intuitive.
"""
class RelayServer(object):
    recieved_image_indices: List[Any]

    def __init__(self, verbosity):
        """ Declares necessary variables. """
        self.t0 = time.time()

        logging.basicConfig(stream=sys.stdout, level=verbosity, format='%(levelname)s: %(message)s')
        # Initialize parameters
        self.RS_path = os.path.dirname(os.path.abspath(inspect.stack()[0][1]))
        self.running = None
        self.latest_pos_index_received = -1
        self.latest_det_index_received = -1  # Counting every received image, assumes they come in the correct order and that no images are lost
        self.end_of_scan = False
        self.end_of_det_stream = False

        self.all_img = {}
        self.all_msg = {}
        self.all_weights = {}
        self.Energy = None
        self.recieved_image_indices = []  # List of received frame indices, read from detector messages
        self.recieved_pos_indices = []
        self.latest_posimg_index_received = []
        self.nr_of_check_replies = 0
        self.energy_replied = False
        self.load_replies = 0
        self.init_params = {}
        self.do_crop = None
        self.do_rebin = None
        self.do_pos_aver = None
        self.do_divide_msgs = False
        self.do_bg_removal = None
        self.do_masking = False
        self.mask_preprocessed = False
        self.center = None
        self.newcenter = None
        self.mask = 'auto'
        self.sendimg = []  ### DEBUG
        self.sendmsg = []  ### DEBUG
        self.auto_rerun = False  # Start listening for new data again after the scan finishes
        self.motors_started = False
        self.detector_started = False
        self.dettottime = 0  ### DEBUG
        self.postottime = 0  ### DEBUG
        self.reltottime = 0  ### DEBUG
        self.reltottimewall = 0  ### DEBUG
        self.zmqtottime = 0  ### DEBUG
        self.zmqtotcputime = 0  ### DEBUG
        print(self.__dict__)

    def connect(self, detector_address, motors_address, relay_address, simulate, simulation_sample=None):
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
        simulation_sample : int, str, or None,
            int: choose which scan file to simulate with from the dictionary in the streamers
            str: with the format of json.dumps({'scan_file': '/.../scanname.hdf5', 'path_to_data': '/hdf5/path/to/data', 'detector': 'detector_name'})
            None: Sample is chosen by the hardcoded

        """
        print(f'Starting Relay server, reading from detector address {detector_address} and positions address {motors_address}')
        self.simulate = simulate
        self.sim_samp = simulation_sample
        context = zmq.Context()
        self.det_socket = context.socket(zmq.PULL)  # data sender
        self.det_socket.connect(detector_address)

        self.pos_socket = context.socket(zmq.SUB)  # data sender
        self.pos_socket.connect(motors_address)
        self.pos_socket.setsockopt(zmq.SUBSCRIBE, b"")

        self.relay_socket = context.socket(zmq.REP)  # client, data consumer
        self.relay_socket.bind(relay_address)

        # Add your detector address here if the diffraction patterns are compressed from byte 12
        self.decomp_from_byte12 = detector_address.rsplit(':', 1)[0] in ['tcp://p-nanomax-eiger-1m-daq.maxiv.lu.se', 'tcp://172.18.10.177', 'tcp://172.18.10.178', 'tcp://172.18.10.188']



    def connect_streamers(self, simulation_sample):
        # Start simulating an ongoing experiment
        os.makedirs(self.RS_path + '/debug_logs', exist_ok=True)
        self.poslogfile = open(self.RS_path + '/debug_logs/motor_log.txt', "w")
        self.detlogfile = open(self.RS_path + '/debug_logs/detector_log.txt', "w")

        if simulation_sample is not None:
            arg_motor = [sys.executable, self.RS_path + '/Simulators/Motor_streamer.py', simulation_sample]
            arg_detector = [sys.executable, self.RS_path + '/Simulators/Detector_streamer.py', simulation_sample]
        else:
            arg_motor = [sys.executable, self.RS_path + '/Simulators/Motor_streamer.py']
            arg_detector = [sys.executable, self.RS_path + '/Simulators/Detector_streamer.py']
        self.rundetector = subprocess.Popen(arg_detector,
                                        stdout=self.detlogfile,
                                        stderr=subprocess.STDOUT)
        self.runmotor = subprocess.Popen(arg_motor,
                                       stdout=self.poslogfile,
                                       stderr=subprocess.STDOUT)


    def run(self):
        # ToDO: Add some assertion/check that sockets have been connected before continuing from here.
        self.i = -1
        self.j = -1
        ######### benchmark
        #self.connect_streamers(simulation_sample=self.sim_samp)
        #########
        self.running = True
        self.cpulooptime0 = time.process_time()
        while self.running:
            try:
                print('')
                print('receiving...')
                print('Received %d positions and %d images\r' % (self.latest_pos_index_received + 1, self.latest_det_index_received + 1), end='')
                t0 = time.perf_counter()  ### DEBUG
                t0cpu = time.process_time()  ### DEBUG

                # Find the first socket
                ready_sockets = zmq.select([self.det_socket, self.pos_socket, self.relay_socket], [], [], None)[
                    0]  # None makes the code wait here until there is something to read.. look at man select!
                t1 = time.perf_counter()  ### DEBUG
                self.zmqtottime += t1 - t0  ### DEBUG
                print(('Time spent in zmq.select() = %f, accumulated time = %f' % ((t1 - t0), self.zmqtottime)))  ### DEBUG
                print('** SELECT:')
                print(ready_sockets)

                # now read from the first ready socket

                if self.det_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    self.det_action()
                    t1 = time.perf_counter()  ### DEBUG
                    self.dettottime += t1 - t0  ### DEBUG
                    logging.debug(('Time spent in det_action() = %f, accumulated time = %f\n' % ((t1 - t0), self.dettottime)))  ### DEBUG

                if self.pos_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    self.pos_action()
                    t1 = time.perf_counter()  ### DEBUG
                    self.postottime += t1 - t0  ### DEBUG
                    logging.debug(('Time spent in pos_action() = %f, accumulated time = %f\n' % ((t1 - t0), self.postottime)))  ### DEBUG

                if self.relay_socket in ready_sockets:
                    t0 = time.perf_counter()  ### DEBUG
                    t00 = time.time()  ### DEBUG
                    # Expects a request of the form: ['check/load', {'frame': int, '**kwargs': value}]
                    print(f'**** RELAY: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
                    request = self.relay_socket.recv_json()
                    print(f'request = {request}')
                    if request[0] == 'preprocess':
                        if self.simulate:
                            # Starting the detector and motor streamers now that we got a connection with the client.
                            logging.debug(self.sim_samp)
                            self.connect_streamers(simulation_sample=self.sim_samp)
                        self.init_params = request[1]
                        self.relay_socket.send_json(['Preprocess message received'])
                        self.do_crop = 'shape' in self.init_params.keys()
                        self.do_rebin = 'rebin' in self.init_params.keys()
                        self.do_pos_aver = 'average_x_at_RS' in self.init_params.keys()  # (Not fully implemented yet)
                        self.do_bg_removal = 'bgfile' in self.init_params.keys()
                        self.do_masking = 'maskfile' in self.init_params.keys()
                        logging.debug(f'got preprocess request: {self.init_params}')
                        if self.do_masking:
                            # This only has to be done once, so we can just do it here.
                            maskfile = self.init_params['maskfile']
                            if 'maskkey' in self.init_params.keys():
                                with h5py.File(maskfile, 'r') as f:
                                    self.mask = f[self.init_params['maskkey']][:]
                            else:
                                with h5py.File(maskfile, 'r') as f:
                                    self.mask = f['mask'][:]
                        if self.do_bg_removal:
                            logging.debug('preprocess: Will perform background removal')
                            self.backgroundfile = self.init_params['bgfile'][0]
                            self.background_path = self.init_params['bgfile'][1]
                            with h5py.File(self.backgroundfile, 'r') as fbg:
                                self.data_background = fbg[self.background_path]
                                self.data_background = np.mean(self.data_background, axis=0)  ## check data type, change to float 32 (or same as diffraction pattern)
                        if self.latest_det_index_received >= 0:
                            logging.debug('preprocess: We got images before we know if and what to preprocess')
                            logging.debug(f'preprocess: latest_det_index_received = {self.latest_det_index_received}')
                            self.all_img_ = self.preprocess(np.array([self.all_img.get(key) for key in self.all_img.keys()]))
                            for k in range(0, self.latest_det_index_received+1):
                                self.all_img[k] = np.array(self.all_img_[k, :, :])
                                if self.do_rebin:
                                    logging.debug(f'self.weight.shape = {self.weight.shape}')  ### DEBUG
                                    self.all_weights[k] = np.array(self.weight[k, :, :])
                                    logging.debug(f'self.all_weights[k] = {self.all_weights[k]}')  ### DEBUG

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
                        logging.debug('inside REQ2')  ### DEBUG
                        frames_accessible = list(set(list(self.all_msg.keys())).intersection(list(self.all_img.keys())))
                        frames_accessible_tot = len(frames_accessible)
                        logging.debug(f'sending [int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream] = {[int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream]}')  ### DEBUG
                        self.relay_socket.send_json([int(frames_accessible_tot), self.end_of_scan and self.end_of_det_stream])
                        self.nr_of_check_replies += 1
                    elif request[0] == 'load':
                        print('load')
                        self.frame_nr = request[1]['frame']
                        # Get the correct indices corresponding to the requested frame_nr
                        sendmsg = [self.all_msg.pop(key) for key in self.frame_nr]
                        # !###sendimg = np.array([self.crop(self.all_img.get(key)) for key in self.frame_nr]) #!# CHANGE get TO POP!
                        sendimg = np.array([self.all_img.pop(key) for key in self.frame_nr])
                        if self.do_crop:
                            if self.load_replies == 0:
                                sendmsg[0]['new_center'] = np.array(self.newcenter)
                                if self.load_replies == 0 and self.mask_preprocessed:
                                    mask_expanded = self.mask[np.newaxis, :, :]
                                    sendimg = np.concatenate([sendimg, mask_expanded], axis=0)
                                    sendmsg[0]['mask_preprocessed'] = True
                                    logging.debug(f"///////// sendimg: {sendimg.shape}, self.mask: {self.mask.shape}")

                        if self.do_rebin:
                            logging.debug(f'self.all_weights.keys = {self.all_weights.keys()}')
                            for key in self.all_weights.keys():
                                logging.debug(f'self.all_weights[{key}].shape = {self.all_weights[key].shape}')
                            sendweight = np.array([self.all_weights.pop(key) for key in self.frame_nr])
                            logging.debug(f'sendweight.shape = {sendweight.shape}')
                            if self.load_replies == 0 and self.do_masking:
                                logging.debug(f"///////// sendimg: {sendimg.shape}, sendweight: {sendweight.shape}, self.mask: {self.mask.shape}")
                                mask_expanded = self.mask[np.newaxis, :, :]
                                sendimg = np.concatenate([sendimg, sendweight, mask_expanded], axis=0)
                                sendmsg[0]['mask_preprocessed'] = True
                            else:
                                sendimg = np.array([sendimg, sendweight])
                            if self.newcenter is not None:
                                sendmsg[0]['new_center'] = np.array(self.newcenter) / float(self.init_params['rebin'])
                            sendmsg[0]['RS_rebinned'] = self.RS_rebinned

                        self.sendimg.append(sendimg)  # !# DEBUG, remove line later
                        self.sendmsg.append(sendmsg)  # !# DEBUG, remove line later
                        sendmsg[0]['dtype'] = sendimg[0].dtype  # Used for decompressing image in LS
                        sendmsg[0]['shape'] = sendimg.shape  # Used for decompressing image in LS
                        self.relay_socket.send_pyobj(sendmsg, flags=zmq.SNDMORE)
                        logging.debug(f'DEBUG, sendimg.shape: {sendimg.shape}, sendimg.dtype: {sendimg.dtype}, sendimg[0].dtype: {sendimg[0].dtype}')
                        self.relay_socket.send(compress_lz4(sendimg), copy=True)
                        self.load_replies += 1
                    elif request[0] == 'stop':
                        self.relay_socket.send_json(['closing connection to relay_socket'])
                        self.stop_outstream()
                    t1 = time.perf_counter()  ### DEBUG
                    t11 = time.time()  ### DEBUG
                    self.reltottime += t1 - t0  ### DEBUG
                    self.reltottimewall += t11 - t00  ### DEBUG
                    logging.debug(('Time spent in if-relay = %f, accumulated time = %f, accululated walltime = %f' % ((t1 - t0), self.reltottime, self.reltottimewall)))  ### DEBUG

                t1cpu = time.process_time()  ### DEBUG
                self.zmqtotcputime += t1cpu - t0cpu  ### DEBUG
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
                raise err

    def det_action(self):
        self.i += 1
        print(f'**** DETECTOR: {time.strftime("%H:%M:%S", time.localtime())}, {time.time() - self.t0} seconds')
        parts = self.det_socket.recv_multipart(copy=False)
        info = json.loads(parts[0].bytes)  ## makes a dict out of info_json
        ### self.all_info.append(info)
        print('info: ', info)

        if not self.motors_started and not self.detector_started:
            if 'filename' in info.keys() and info['filename'] != '':
                logging.debug('This should be the initial message of a scan')  ### DEBUG
                print('********************* DETECTOR STARTING')
                self.detector_started = True
                #### Benchmarking
                if not self.motors_started:
                    self.cpureceivetime0 = time.process_time()
                    self.receivetime0 = time.perf_counter_ns()
            else:
                logging.debug('Image or message from Live-mode or already started scan')  ### DEBUG
        else:
            if info['htype'] == 'image':
                print(f'parts[1].buffer.nbytes = {parts[1].buffer.nbytes}')
                ##################################
                ## FOR NANOMAX
                ##################################
                # Fix for dealing with bitshuffle weirdness;
                # the bitshuffle from the modules at MAXIV differs slightly from the one available
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
                    ## FOR SOFTIMAX detectors (no compression), except for their new (2025) eiger.
                    ##################################
                    img = np.frombuffer(parts[1].buffer, dtype=np.dtype(info['type'])).reshape(info['shape'])
                    ##################################
                else:
                    print(f"Unknown compression type: {info['compression']}")

                # %#self.all_img.append(img)
                self.latest_det_index_received += 1

                # preprocess data and check whether the preprocess request from relay_socket have been received
                logging.debug(f'##############img shape: {img.shape}')  ### DEBUG
                if self.do_crop or self.do_rebin:
                    img = self.preprocess(img)

                self.all_img[self.latest_det_index_received] = img  ##[info['frame']] = img ##
                self.recieved_image_indices.append(info['frame'])
            elif info['htype'] == 'series_end':
                self.end_of_det_stream = True
                print('End of detector stream')


    def preprocess(self, prepimg, keys=None):
        """
        The cropping is performed before the rebinning, just like in PtyPy.
        The mask is cropped and rebinned when the preprocess request has come in.

        :param prepimg: ndarray of image/s to preprocess.
        :param keys: list of frame keys that weights should be stored in.
            Only used when rebinning should be made and frames were recieved before
            we got the preprocess request.
        :return: prepimg: ndarray of preprocessed image/s.
        """


        prepmsg = self.all_msg
        if self.do_bg_removal:
            prepimg = prepimg - self.data_background

        if self.do_crop:
            prepimg, self.newcenter, self.padmask = self.crop(prepimg)
            if self.do_masking and not self.mask_preprocessed and ('center' not in self.init_params.keys() or self.init_params['center'] is None):
                # Then we need do crop the mask with the auto-found center and send it too.
                self.mask, self.newcenter_, self.padmask_ = self.crop(self.mask)
                self.mask_preprocessed = True

        if self.do_rebin:
            if self.do_masking and self.do_crop and not self.mask_preprocessed:
                self.mask, self.newcenter__, self.padmask__ = self.crop(self.mask)
                self.mask = u.rebin_2d(self.mask, self.init_params['rebin'])[0]
                self.mask_preprocessed = True

            try:
                self.weight = np.ones_like(prepimg)
                self.weight[np.where(prepimg == 2 ** 32 - 1)] = 0
                self.weight = u.rebin_2d(self.weight, self.init_params['rebin'])
                self.all_weights[self.latest_det_index_received] = self.weight[0]  # the rebinning function adds an extra dimension to the array
                prepimg = u.rebin_2d(prepimg, self.init_params['rebin'])[0]
                logging.debug(f'weight.shape = {self.weight.shape}, prepimg.shape = {prepimg.shape}')  ### DEBUG
                self.RS_rebinned = True
            except:
                print('Warning: could not rebin, leaving this task to PtyPy instead.')
                self.RS_rebinned = False
        return prepimg

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
                if 'wffermat ' in msg['description'] and not self.simulate:
                    # Then all positions will come in a single msg
                    self.do_divide_msgs = True
                if 'csnake' in msg['description']:
                    # We don't want the csnake data! NOTE: STILL HAVE TO FIX THIS PART FOR THE DETECTOR!
                    self.detector_started = False
                    self.all_img = {}
                else:
                    self.motors_started = True
                    #### Benchmark
                    if not self.detector_started:
                        self.cpureceivetime0 = time.process_time()
                        self.receivetime0 = time.perf_counter_ns()
            else:
                print('Message was not important')
        else:
            if msg['status'] == 'running':
                self.j += 1
                self.latest_pos_index_received += 1
                self.all_msg[self.j] = msg
                if self.do_divide_msgs:
                    print('Will divide position message!')
                    self.all_msg = self.divide_burst_msg(msg)
                    self.latest_pos_index_received = len(self.all_msg)-1
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
        """
        Used when all positions will come in a single msg.
        :param msg_burst: {dict} the received msg containing all positions
        :return:
        msgs_divided: {dict} divided dict which looks like the positions
            came in separate messages (like in the most common way).
        """
        xMotor = 'pseudo/x'  ### HARDCODING
        xMotorKeys = xMotor.split('/')  ### HARDCODING
        n_burst = msg_burst[xMotorKeys[0]][xMotorKeys[1]].__len__()
        nr = np.linspace(0, n_burst - 1, n_burst, dtype=int)
        msgs_divided = dict(zip(nr, map(lambda i: self.divide_msgs(copy.deepcopy(msg_burst), i), nr)))
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
        self.cpulooptime1 = time.process_time()
        self.cpureceivetime1 = time.process_time()
        self.receivetime1 = time.perf_counter_ns()
        print(f'Closing self.det_socket, self.pos_socket! at time {time.strftime("%H:%M:%S", time.localtime())}')
        print('Received %d pos, %d img' % (self.latest_pos_index_received + 1, self.latest_det_index_received + 1))
        print(f'total cpu receive time: {self.cpureceivetime1-self.cpureceivetime0}, total receive time: {(self.receivetime1-self.receivetime0)*10**(-9)}')
        print(f'total cpu time: {self.zmqtotcputime}, cpulooptime: {self.cpulooptime1-self.cpulooptime0}')
        print(f'RS.dettottime  {self.dettottime}\nRS.postottime  {self.postottime}\nRS.zmqtottime  {self.zmqtottime}')
        self.det_socket.close()
        self.pos_socket.close()
        if self.simulate and not self.energy_replied:
            # Then the relay_socket was never connected and we can stop everything.
            self.stop_outstream()

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

        Parameters
        ----------
        diff: ndarray of image/s to crop.
        get_weights

        Returns
        -------
        diff_croppad: ndarray of cropped diff
        c_a: the new center coordinate
        padwidth

        """

        # Only allow square slices in data
        ####self.init_params['shape'] = np.min(self.init_params['shape'])

        # a: desired crop shape, b: resulting crop shape, c: center , d: diff shape

        # Keep full (y, x) shape — do NOT force square
        if isinstance(self.init_params['shape'], (list, tuple, np.ndarray)):
            a_sh = np.array(self.init_params['shape'], dtype=int)  # (ay, ax)
        else:
            self.init_params['shape'] = np.min(self.init_params['shape'])
            a_sh = np.array([self.init_params['shape'], self.init_params['shape']], dtype=int)


        if self.center is None:
            if 'center' in self.init_params.keys() and isinstance(self.init_params['center'], (list, tuple)):
                c_d = np.array(self.init_params['center'], dtype=int)  ## cy, cx : row, col
            else:

                c_d = np.rint(self.find_center(diff, self.mask)).astype(int)
                logging.debug(f'Found center at {c_d}')
            self.center = c_d
        else:
            c_d = self.center

        # Find limits for cropping
        d_sh = diff.shape[-2:]  # (rows, cols) (yspan, xspan)
        #####a_sh = int(self.init_params['shape'])

        a_lowlims = c_d - a_sh // 2  # !##  array([367, -41])
        a_highlims = c_d + (a_sh + 1) // 2  # (a_sh + 1) to enable uneven nr of pixels.

        # Set the limits within the boundaries of diffraction pattern
        b_lowlims = np.max((a_lowlims, np.zeros(2, dtype='int')), axis=0)
        b_highlims = np.min((a_highlims, np.array(d_sh)), axis=0)
        lims = np.array([b_lowlims, b_highlims])

        ##diff_crop = diff[b_lowlims[0]:b_highlims[0], b_lowlims[1]:b_highlims[1]]
        diff_crop = np.ascontiguousarray(np.split(np.split(diff, lims[:, 0], axis=-2)[1], lims[:, 1], axis=-1)[1])  # using ascontiguousarray automatically means that you'll make a copy so the array can be stored contiguously.

        ###c_a = np.array([a_sh // 2, a_sh // 2])
        c_a = a_sh // 2
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
        If preprocess info is recieved before any data, it will calculate the center
        based on the first diffraction pattern. Otherwise, it will be calculated based
        on all the patterns that's been loaded when the preprocess info comes in.

        Center is calculated using all available diffraction patterns at the same time,
        meaning that if one diffraction pattern would have half of the intensity
        of a second diffraction pattern, then it will also contribute half as
        much, compared to the second one.

        Parameters
        ----------
        diff : ##dict of 2D ndarrays
        mask : 2D ndarray

        Returns
        -------

        """
        if isinstance(diff, str):
            diff = self.all_img
        if isinstance(diff, dict):
            diff = np.array([diff[key] for key in diff.keys()])
        axes = tuple(range(1, diff.ndim + 1))

        if isinstance(mask, str) and mask == 'auto':
            mask = np.ones_like(diff[0])
            mask[np.where(diff[0] == 2 ** 32 - 1)] = 0

        if mask is None:
            return (np.sum(diff * np.indices(diff.shape), axis=axes, dtype=float) / np.sum(diff, dtype=float))[-2:]
        else:
            return (np.sum(diff * mask * np.indices(diff.shape), axis=axes, dtype=float) / np.sum(diff * mask, dtype=float))[-2:]

    def average_positions(self, sendmessage):
        """
        (Not fully implemented yet)
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
    """
    Launches the RelayServer.
    """
    if RS is None:
        RS = RelayServer(verbosity=logging.DEBUG) # choices: logging.INFO or logging.DEBUG
    # info about which hosts and ports to use are in gitlab>streaming-receiver>detector-config.json
    #     https://gitlab.maxiv.lu.se/scisw/detectors/streaming-receiver-cpp
    # since sometime before 2025: https://k8s.maxiv.lu.se/dashboard/c/c-m-gsgx4zjk/explorer/apps.deployment/softimax-dectris-eiger/softimax-dectris-eiger-daq#pods
    known_sources = {'Simulator':       {'det_adr': 'tcp://0.0.0.0:56789', 'pos_adr': 'tcp://127.0.0.1:5556'},
                     # 'NanoMAX_eiger1M': {'det_adr': 'tcp://b-daq-node-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     #                   'NanoMAX_eiger1M': {'det_adr': 'tcp://p-daq-cn-2:20007', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     #'NanoMAX_eiger4M': {'det_adr': 'tcp://b-daq-node-2:20001', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     #'NanoMAX_eiger1M': {'det_adr': 'tcp://p-nanomax-eiger-1m-daq.maxiv.lu.se:5556', 'pos_adr': 'tcp://172.16.125.30:5556'},
                     #'NanoMAX_eiger4M': {'det_adr': 'tcp://p-nanomax-eiger-4m-daq.maxiv.lu.se:5556', 'pos_adr': 'tcp://172.16.126.61:5556'},
                     'NanoMAX_eiger1M': {'det_adr': 'tcp://172.18.10.178:5556', 'pos_adr': 'tcp://172.16.126.61:5556'},
                     'NanoMAX_eiger4M': {'det_adr': 'tcp://172.18.10.177:5556', 'pos_adr': 'tcp://172.16.126.61:5556'},  # updated 10 Feb 2025
                     ## pos_adr for NanoMAX can also be: 'tcp://b-nanomax-controlroom-cc-3:5556'
                     # need to login to blue network - SOFTIMAX to connect to det and pos
                     'SoftiMAX_andor':  {'det_adr': 'tcp://p-fanout-softimax-xzyla-andor3:10000', 'pos_adr': 'tcp://172.16.205.5:5556'},  # det_adr: tcp://b-softimax-cams-0:20007, pos_adr: b-softimax-cc-0
                     'SoftiMAX_eiger': {'det_adr': 'tcp://172.18.10.188:5556', 'pos_adr': 'tcp://172.16.205.9:5556'}
                     # SMeiger tried: 172.20.13.210, 172.20.0.73, 172.20.7.182, 172.20.10.157, 172.20.12.59, 172.18.10.45, 172.20.12.213
                     # http://172.18.10.44:1954/, http://172.18.10.44:30001/, http://172.18.10.44:30002/
                     # 172.16.206.109:5556 = b-softimax-cams-dectris-0.maxiv.lu.se
                     # 172.20.13.210, 172.18.10.188
                     }
    #src = known_sources['SoftiMAX_eiger']
    src = known_sources['Simulator']
    relay_adr = 'tcp://127.0.0.1:45678'
    if len(sys.argv) >= 2:
        # Sample have been chosen through input
        simsample = sys.argv[1]
        print(f'simsample: {simsample}')
    else:
        simsample = None

    RS.connect(detector_address=src['det_adr'], motors_address=src['pos_adr'], relay_address=relay_adr, simulate=True, simulation_sample=simsample)
    RS.run()

    return known_sources, src, relay_adr, RS


if __name__ == "__main__":
    known_sources, src, relay_adr, RS = launch()
    if RS.simulate:
        RS.poslogfile.close()
        RS.detlogfile.close()
        # ## Uncomment below for printing debug logs of the stream simulators.
        # motor_out = RS.runmotor.communicate()[0].decode().split('\n')
        # detector_out = RS.rundetector.communicate()[0].decode().split('\n')
        with open(RS.poslogfile.name, 'r') as f1:
            motor_out = f1.read().split('\n')
        with open(RS.detlogfile.name, 'r') as f2:
            detector_out = f2.read().split('\n')
        try:
            ind_prepmot = [i for i, s in enumerate(motor_out) if "Prepping" in s][0]
            ind_finmot = [i for i, s in enumerate(motor_out) if "Finished" in s][0]
            print(f'\nMotor-log:\n{motor_out[ind_prepmot]}\n{motor_out[ind_finmot]}')
        except:
            print('Error: The motor-log did not report as expected.')
        try:
            ind_prepdet = [i for i, s in enumerate(detector_out) if "Prepping" in s][0]
            ind_findet = [i for i, s in enumerate(detector_out) if "Finished" in s][0]
            print(f'Detector-log:\n{detector_out[ind_prepdet]}\n{detector_out[ind_findet]}')
        except:
            print('Error: The detector-log did not report as expected.')
        # # print(detector_out[:10])
        # # print(motor_out[:10])
        # # print(detector_out[-10:])
        # # print(motor_out[-10:])
