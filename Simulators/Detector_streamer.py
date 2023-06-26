import zmq
from bitshuffle import compress_lz4
import numpy as np
import re
from ptypy import io
import errno
import time

"""
Script used to simulate the data stream from the Eiger detector.
"""
t0 = time.time()
print('Starting Eiger detector simulator')
scans = {0: {'scan_file': '/data/visitors/nanomax/20220196/2022040308/raw/mar29_KB_align/scan_000490_eiger.hdf5', 'detector': 'eiger'},
         1: {'scan_file': '/data/visitors/nanomax/20211244/2021120808/raw/0000_setup/scan_000036_eiger.hdf5', 'detector': 'eiger'},
         2: {'scan_file': '/home/reblex/Documents/Data/SavedRelayMessages/20220824/raw/sample/scan_000029_eiger1m.hdf5', 'detector': 'eiger1m'},
         3: {'scan_file': '/home/reblex/Documents/Data/nanomax_siemens_KB/scan_000006_eiger.hdf5', 'detector': 'eiger'},
         4: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000038_eiger1m.hdf5', 'detector': 'eiger1m'},
         5: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/scan_000040_eiger1m.hdf5', 'detector': 'eiger1m'},
         6: {'scan_file': '/home/reblex/Documents/Data/NM_livebeam_2022-11-01/sliced_scan_000040/scan_000040_eiger1m.hdf5', 'detector': 'eiger1m'}
         }
sample = 6  ######## Pick your sample here! 0:27fr, 1:1000fr, 2:16fr, 3:100fr, 4:55fr, 5:55fr, 6:15fr
scan_fname = scans[sample]['scan_file']
path, scannr = re.findall(r'/.{0,}/|\d{6}', scan_fname)
data = io.h5read(scan_fname, 'entry')['entry']['measurement']['Eiger']['data']

imshape = data.shape[-2:]  ## (1062, 1028) ##(5,4)  ## (2162,2068) , (1062, 1028)
npx = imshape[0] * imshape[1]
nframes = data.shape[0]

## find a free port in the terminal with:
## python -c "import socket; s = socket.socket(); s.bind(('', 0));print(s.getsockname()[1]);s.close()"
## 35551

det_host = 'tcp://0.0.0.0'
det_port = '56789'

context = zmq.Context()
det_socket = context.socket(zmq.PUSH)
## Check if port is already in use: (when we're using a random port for testing we don't wan't it to already be in use)
## ToDo: if port is already in use and it's occupied by user 'reblex', then kill that procces.

try:
    det_socket.bind(f'{det_host}:{det_port}')
    print(f'det_socket now bound to {det_host}:{det_port}')
except zmq.error.ZMQError as e:
    if e.errno == errno.EADDRINUSE:
        print("Port is already in use")
    else:
        # something else raised the socket.error exception
        print(e)
######## If you're sure port is available you'd simply use:
####det_socket.bind('tcp://0.0.0.0:56789') ## 'tcp://b-daq-node-2:20001'

###time.sleep(2)  # naive wait for clients to arrive

i = -1
k = 438  ## sort of random nr, depends on how many runs have been made previously.
dct_first = {'filename': scan_fname, 'htype': 'header', 'msg_number': k}
t1 = time.time()
print(f'Prepping the eiger simulator took {t1 - t0:.04f} s.')  ## 1.0029 s
print(f'Starting at time {time.strftime("%H:%M:%S", time.localtime())}')
det_socket.send_json(dct_first)
###time.sleep(0.2)
while True:
    i += 1
    k += 1
    dct = {"compression": "bslz4", "frame": i, "htype": "image", "msg_number": k, "shape": [imshape[0], imshape[1]], "type": "uint32"}
    data_array = np.ndarray(shape=imshape, dtype=data.dtype,
                            buffer=data[i, :, :])  ## buffer=np.linspace(i, npx-1+i, npx, dtype="uint32")) ## buffer=np.random.rand(imshape[0],imshape[1]))
    data_array_compressed = compress_lz4(data_array)
    det_socket.send_json(dct, flags=zmq.SNDMORE)
    det_socket.send(
            data_array_compressed)  # in the meeting 1april, we said that I should have data_array.buffer as the input here but then I just get the error:  'numpy.ndarray' object has no attribute 'buffer'
    print(f'Sent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}')
    print(f'---- data_array = {data_array}')
    print(f'---- data_array_compressed = {data_array_compressed}')
    ###time.sleep(0.2)
    if i == nframes - 1:
        i += 1
        k += 1
        dct_last = {'htype': 'series_end', 'msg_number': k}
        det_socket.send_json(dct_last)
        print(f'Finished at time {time.strftime("%H:%M:%S", time.localtime())}')
        break

###time.sleep(10)
det_socket.close()