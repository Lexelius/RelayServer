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
from ptypy import utils as u   ###DEBUG
logger = u.verbose.logger   ###DEBUG
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

scans = {0: {'scan_file': '/data/visitors/nanomax/20220196/2022040308/raw/mar29_KB_align/scan_000490_eiger.hdf5',           'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger'},    # 27 frames
         1: {'scan_file': '/data/visitors/nanomax/20211244/2021120808/raw/0000_setup/scan_000036_eiger.hdf5',               'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger'},    # 1000 frames
         2: {'scan_file': '/home/reblex/Documents/Data/SavedRelayMessages/20220824/raw/sample/scan_000029_eiger1m.hdf5',    'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger1m'},  # 16 frames
         3: {'scan_file': '/home/reblex/Documents/Data/nanomax_siemens_KB/scan_000006_eiger.hdf5',                          'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger'},    # 100 frames
         4: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000038_eiger1m.hdf5',                    'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger1m'},  # 55 frames
         5: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000040_eiger1m.hdf5',                    'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger1m'},  # 55 frames
         6: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/sliced_scan_000040/scan_000040_eiger1m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger1m'},  # 15 frames
         7: {'scan_file': '/data/visitors/softimax/20230687/2023092608/raw/20230930/scan_000754_andor.h5',                  'path_to_data': '/entry/instrument/zyla/data',   'detector': 'andor'},    # 11 frames, just a loopscan
         8: {'scan_file': '/data/visitors/softimax/20230687/2023092608/raw/20230930/scan_000778_andor.h5',                  'path_to_data': '/entry/instrument/zyla/data',   'detector': 'andor'},    # 200 frames, spiralscan, burst=1
         9: {'scan_file': '/data/visitors/softimax/20230687/2023092608/raw/20230927/scan_000118_andor.h5',                  'path_to_data': '/entry/instrument/zyla/data',   'detector': 'andor'},    # 1046 frames, fermatscan, burst=1, Has a good off-line reconstruction!
         10: {'scan_file': '/data/visitors/softimax/20230687/2023092608/raw/20230930/scan_000853_andor.h5',                 'path_to_data': '/entry/instrument/zyla/data',   'detector': 'andor'},    # 220 frames, fermatscan, burst=1, haven't checked recons
         11: {'scan_file': '/data/visitors/softimax/20230687/2023092608/raw/20230930/scan_000702_andor.h5',                 'path_to_data': '/entry/instrument/zyla/data',   'detector': 'andor'},    # 306 frames, fermatscanoff, burst=1, haven't checked recons
         12: {'scan_file': '/data/visitors/nanomax/20211244/2023020108/raw/0003_setup/scan_001190_eiger4m.hdf5',            'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'}   # 2912 frames, fermatscan
         }
sample = 12  ######## Pick your sample here! 0:27fr, 1:1000fr, 2:16fr, 3:100fr, 4:55fr, 5:55fr, 6:15fr
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
            elif k in dct.keys():
                d[k] = v[i:i + 1][0]  # store values at top level as float and not as arrays
            else:
                d[k] = v[i:i + 1]
    dct['status'] = 'running'
    return dct


# Correcting the ['eiger']['frames'] outside divide_msgs to avoid unnecessary copy:
# I don't think there's a way to tell if universal should be True or False when reading the files.
msgs_prepped = msgs.copy() ## deepcopy unneccessary her and takes alot of time! # copy.deepcopy(msgs)
if 'eiger' in scans[sample]['detector']:
    msgs_prepped[scans[sample]['detector']]['frames'] = {'type':      'Link',
                                                         'filename':  scan_fname,
                                                         'path':      scans[sample]['path_to_data'], #'entry/measurement/Eiger/data',
                                                         'universal': True}
elif 'andor' in scans[sample]['detector']:
    msgs_prepped[scans[sample]['detector']] = {'type':      'Link',
                                               'filename':  scan_fname,
                                               'path':      scans[sample]['path_to_data'],
                                               'universal': True}

msgs_divided = list(map(lambda i: divide_msgs(copy.deepcopy(msgs_prepped), i), nr))
## Using scans[0] with 27 frames gave:
# %timeit msgs_divided = list(map(lambda i: divide_msgs(copy.deepcopy(msgs_prepped), i), nr))
# 4.17 ms ± 342 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)

if scans[sample]['detector'] == 'eiger4m':
    initial_msg = {'scannr': scannr, 'status': 'started', 'path': path.rstrip('/'), 'snapshots': h5_data['snapshots']['pre_scan'], 'description': h5_data['description']}
    last_msg = {'scannr': scannr, 'status': 'finished', 'path': path.rstrip('/'), 'snapshots': h5_data['snapshots']['post_scan'], 'description': h5_data['description']}
    # Store values as float and not as arrays (as it is in the streamed messages):
    snap_pre = h5_data['snapshots']['pre_scan'].copy()
    snap_post = h5_data['snapshots']['post_scan'].copy()
    for key, val in snap_pre.items():
        snap_pre[key] = val[0]
    for key, val in snap_post.items():
        snap_post[key] = val[0]
else:
    initial_msg = {'scannr': scannr, 'status': 'started', 'path': path.rstrip('/'), 'snapshot': h5_data['snapshot'], 'description': h5_data['description']}
    last_msg = {'scannr': scannr, 'status': 'finished', 'path': path.rstrip('/'), 'snapshot': h5_data['snapshot'], 'description': h5_data['description']}
    # Store values as float and not as arrays (as it is in the streamed messages):
    snap = h5_data['snapshot'].copy()
    for key, val in snap.items():
        snap[key] = val[0]

# Store values as float and not as arrays (as it is in the streamed messages):
# snap = h5_data['snapshot'].copy()
# for key, val in snap.items():
#     snap[key] = val[0]

initial_msg = {'scannr': int(scannr),
               'status': 'started',
               'path': path.rstrip('/'),
               'snapshot': snap_pre if scans[sample]['detector'] == 'eiger4m' else snap,
               'description': h5_data['description'][0].decode('utf-8')}

last_msg = {'scannr': int(scannr),
            'status': 'finished',
            'path': path.rstrip('/'),
            'snapshot': snap_post if scans[sample]['detector'] == 'eiger4m' else snap,
            'description': h5_data['description'][0].decode('utf-8')}

# %% Start sending data

###time.sleep(1.3)  # naive wait for clients to arrive
t1 = time.time()
print(f'Prepping the contrast simulator took {t1 - t0:.04f} s.')
print(f'Starting at time {time.strftime("%H:%M:%S", time.localtime())}')
pos_socket.send_pyobj(initial_msg)
time.sleep(0.2)

i = -1
for i in range(nframes):
    pos_socket.send_pyobj(OrderedDict(msgs_divided[i]))
    #logger.log(12345, f'Sent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}')
    print(f'Sent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}', flush=True)
    #time.sleep(0.6)

pos_socket.send_pyobj(last_msg)
print(f'Finished at time {time.strftime("%H:%M:%S", time.localtime())}')

###time.sleep(10)  # naive wait for tasks to drain
pos_socket.close()