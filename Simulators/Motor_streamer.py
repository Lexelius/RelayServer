"""
ToDo:
* Make it work for multiple detectors.
*
"""
import zmq
import numpy as np
from collections import OrderedDict
import copy
import re
from ptypy import io
import time

# %%
"""
Script used to simulate the stream containing motor related data, such as positions and energy.
"""
t0 = time.time()
print('Starting Contrast simulator')
pos_host = 'tcp://127.0.0.1'  # NanoMax contrast: 'tcp://172.16.125.30'
pos_port = '5556'  # NanoMax contrast: '5556'

context = zmq.Context()
pos_socket = context.socket(zmq.PUB)
pos_socket.bind(f'{pos_host}:{pos_port}')  ## ("tcp://172.16.125.30:5556")# ("tcp://b-nanomax-controlroom-cc-3:5556")

# %% Pre-load the data that will be used to simulate the stream

scans = {0: {'scan_file': '/data/visitors/nanomax/20220196/2022040308/raw/mar29_KB_align/scan_000490_eiger.hdf5', 'detector': 'eiger'},
         1: {'scan_file': '/data/visitors/nanomax/20211244/2021120808/raw/0000_setup/scan_000036_eiger.hdf5', 'detector': 'eiger'},
         2: {'scan_file': '/home/reblex/Documents/Data/SavedRelayMessages/20220824/raw/sample/scan_000029_eiger1m.hdf5', 'detector': 'eiger1m'},
         3: {'scan_file': '/home/reblex/Documents/Data/nanomax_siemens_KB/scan_000006_eiger.hdf5', 'detector': 'eiger'},
         4: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000038_eiger1m.hdf5', 'detector': 'eiger1m'},
         5: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000040_eiger1m.hdf5', 'detector': 'eiger1m'},
         6: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/sliced_scan_000040/scan_000040_eiger1m.hdf5', 'detector': 'eiger1m'}
         }
sample = 6  ######## Pick your sample here!
scan_fname = scans[sample]['scan_file']
path, scannr = re.findall(r'/.{0,}/|\d{6}', scan_fname)  # ToDo: use os.path.split(scan_fname)
h5_fname = path + scannr + '.h5'

h5_data = io.h5read(h5_fname, 'entry')['entry']
msgs = OrderedDict(reversed(list(h5_data['measurement'].items())))
nframes = msgs['dt'].__len__()
nr = np.linspace(0, nframes - 1, nframes, dtype=int)


# %% Rearranging of h5_data['measurement'] into separate messages/frames
# Assuming that all values that are not dicts are ndarrays of size equal to nr of frames.

# walk_dict() copied from contrast's StreamRecorder.py
def walk_dict(dct):
    """
    A recursive version of dict.items(), which yields
    (containing-dict, key, val).
    """
    for k, v in dct.items():
        yield dct, k, v
        if isinstance(v, dict):
            for d_, k_, v_ in walk_dict(v):
                yield d_, k_, v_


# Extracts a single message from the dict-array of messages
def divide_msgs(dct, i):
    for d, k, v in walk_dict(dct):
        if isinstance(v, np.ndarray):
            if k == 'thumbs:':
                d[k] = v[i].base  # converts b'None' into None
            else:
                d[k] = v[i:i + 1]
    dct['status'] = 'running'
    return dct


# Correcting the ['eiger']['frames'] outside divide_msgs to avoid unnecessary copy:
msgs_prepped = copy.deepcopy(msgs)
msgs_prepped[scans[sample]['detector']]['frames'] = {'type':      'Link',
                                                     'filename':  scan_fname,
                                                     'path':      'entry/measurement/Eiger/data',
                                                     'universal': True}

msgs_divided = list(map(lambda i: divide_msgs(copy.deepcopy(msgs_prepped), i), nr))
## Using scans[0] with 27 frames gave:
# %timeit msgs_divided = list(map(lambda i: divide_msgs(copy.deepcopy(msgs_prepped), i), nr))
# 4.17 ms ± 342 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)


# %% Start sending data

initial_msg = {'scannr': scannr, 'status': 'started', 'path': path.rstrip('/'), 'snapshot': h5_data['snapshot'], 'description': h5_data['description']}
last_msg = {'scannr': scannr, 'status': 'finished', 'path': path.rstrip('/'), 'snapshot': h5_data['snapshot'], 'description': h5_data['description']}

###time.sleep(1.3)  # naive wait for clients to arrive
t1 = time.time()
print(f'Prepping the contrast simulator took {t1 - t0:.04f} s.')
print(f'Starting at time {time.strftime("%H:%M:%S", time.localtime())}')
pos_socket.send_pyobj(initial_msg)
###time.sleep(0.2)

i = -1
for i in range(nframes):
    pos_socket.send_pyobj(OrderedDict(msgs_divided[i]))
    print(f'Sent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}')
###    time.sleep(0.2)

pos_socket.send_pyobj(last_msg)
print(f'Finished at time {time.strftime("%H:%M:%S", time.localtime())}')

###time.sleep(10)  # naive wait for tasks to drain
pos_socket.close()