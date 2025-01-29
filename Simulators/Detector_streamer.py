import zmq
from bitshuffle import compress_lz4
import numpy as np
import re
from ptypy import io
import errno
import time

"""
Script used to simulate the data stream from an Eiger or Andor detector.
"""
t0 = time.time()
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
         12: {'scan_file': '/data/visitors/nanomax/20211244/2023020108/raw/0003_setup/scan_001190_eiger4m.hdf5',            'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 2912 frames, fermatscan
         13: {'scan_file': '/data/visitors/nanomax/20211244/2023120608/raw/0004_MIH/scan_003187_eiger4m.hdf5',              'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},   # 132 frames, fermatscan, sick tooth, part of ptycho-tomo
         14: {'scan_file': '/data/visitors/nanomax/20211244/2023120608/raw/0004_MIH/scan_000152_eiger4m.hdf5',              'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},   # 237 frames, fermatscan, sick tooth
         15: {'scan_file': '/data/staff/nanomax/commissioning_2023-2/20231211_tests/raw/0005_MA71/scan_000069_eiger4m.hdf5', 'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 132 frames, wffermat, scanned from left to right as before, siemens, stepsize 1.5
         16: {'scan_file': '/data/staff/nanomax/commissioning_2023-2/20231211_tests/raw/0005_MA71/scan_000070_eiger4m.hdf5', 'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 132 frames, wffermat, siemens, stepsize 1.5
         17: {'scan_file': '/data/staff/nanomax/commissioning_2023-2/20231211_tests/raw/0005_MA71/scan_000071_eiger4m.hdf5', 'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 296 frames, wffermat, siemens, stepsize 1.0
         18: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_000004_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 132 frames, fermatscan, electrode
         19: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001103_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         20: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001209_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         21: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001233_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         22: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001243_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         23: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001297_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         24: {'scan_file': '/data/visitors/nanomax/20220550/2023121308/raw/0002_3wt/scan_001383_eiger4m.hdf5',               'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 292 frames, wffermat, electrode
         25: {'scan_file': '/data/visitors/nanomax/20211244/2023120608/raw/0003_Au250Ag100/scan_000138_eiger4m.hdf5',        'path_to_data': '/entry/instrument/Eiger/data', 'detector': 'eiger4m'},  # 670 frames, fermatscan, Gold and silver nanoparticles
         26: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/NTT_scan_001190/simulated_data/sim_files_04/scan_001190_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 1257 frames, simulated data from recon of sample nr 12.
         27: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/NTT_scan_001190/simulated_data/sim_files_05/scan_001190_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 2912 frames, simulated data from recon of sample nr 12. original positions
         28: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/NTT_scan_001190/simulated_data/sim_files_06/scan_001190_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 2912 frames, simulated data from recon of sample nr 12. original positions wrong order
         29: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/NTT_scan_001190/simulated_data/sim_files_07/scan_001190_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 2912 frames, simulated data from recon of sample nr 12, original positions, correct orientation
         30: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/NTT_scan_001190/simulated_data/sim_files_256px_08/scan_001190_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 2912 frames, simulated data from recon of sample nr 12, original positions, correct orientation, 256px
         31: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_64px_Au-Si3N4_gauss_step10px_r40_1e+12_01/scan_000000_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 400 frames, simulated data from constructed siemens star array and gaussian probe
         32: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step10px_1e+12_00/scan_000000_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 400 frames, raster, simulated data from constructed siemens star array and probe from NTT_scan_001190
         33: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step10px_1e+10_poisTRUE_spiral_00/scan_000000_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 315 frames, spiral, simulated data from constructed siemens star array and probe from NTT_scan_001190
         34: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step40px_1e+10_poisTRUE_spiral_00/scan_000000_eiger4m.hdf5', 'path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},  # 315 frames, spiral, simulated data from constructed siemens star array and probe from NTT_scan_001190
         35: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step35px_1e+10_poisTRUE_spiral_00/scan_000000_eiger4m.hdf5','path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},   # 315 frames, spiral, simulated data from constructed siemens star array and probe from NTT_scan_001190
         36: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step27px_1e+10_poisTRUE_spiral_00/scan_000000_eiger4m.hdf5','path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},   # 315 frames, spiral, simulated data from constructed siemens star array and probe from NTT_scan_001190
         37: {'scan_file': '/data/staff/nanomax/reblex/data-simulated-recons/Siemens-img/simulated_data/sim_files_simg_256px_Au-Si3N4_step19px_1e+10_poisTRUE_spiral_00/scan_000000_eiger4m.hdf5','path_to_data': '/entry/measurement/Eiger/data', 'detector': 'eiger4m'},   # 315 frames, spiral, simulated data from constructed siemens star array and probe from NTT_scan_001190
         }
sample = 37  ######## Pick your sample here! 0:27fr, 1:1000fr, 2:16fr, 3:100fr, 4:55fr, 5:55fr, 6:15fr
sleeptime = 0.6+0.0415#0.6 #for sample 29#0.5  # Time taken between sending the detector messages
prepsleep = 0.289#1.57 for sample 30#0. for sample 29 # Make up for difference in prepping time of the detector- and motor streamer
scan_fname = scans[sample]['scan_file']
path, scannr = re.findall(r'/.{0,}/|\d{6}', scan_fname)
data = io.h5read(scan_fname, scans[sample]['path_to_data'])[scans[sample]['path_to_data']]
##data=np.flip(data,0) # uncomment for flipping the order
compr = 'bslz4' if 'eiger' in scans[sample]['detector'] else 'none'

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

time.sleep(prepsleep)  # Make up for difference in prepping time of the detector- and motor streamer
i = -1
k = 438  ## sort of random nr, depends on how many runs have been made previously.
dct_first = {'filename': scan_fname, 'htype': 'header', 'msg_number': k}
t1 = time.time()
print(f'Prepping the detector simulator took {t1 - t0:.04f} s.')  ## 1.0029 s
print(f'Starting at time {time.strftime("%H:%M:%S", time.localtime())}')
det_socket.send_json(dct_first)
while True:
    try:
        if not sleeptime == 0: time.sleep(sleeptime)
        i += 1
        k += 1
        dct = {"compression": compr, "frame": i, "htype": "image", "msg_number": k, "shape": [imshape[0], imshape[1]], "type": str(data.dtype)}
        data_array = np.ndarray(shape=imshape, dtype=data.dtype,
                                buffer=data[i, :, :])  ## buffer=np.linspace(i, npx-1+i, npx, dtype="uint32")) ## buffer=np.random.rand(imshape[0],imshape[1]))

        if compr == 'bslz4':
            ## It takes apprx 0.05s for 1 iteration (without sleeping) with this setting when
            ## data_array.shape: [2162, 2068],  and data_array.dtype: dtype('uint32')
            data_array_compressed = compress_lz4(data_array)
            det_socket.send_json(dct, flags=zmq.SNDMORE)
            det_socket.send(data_array_compressed)  # in the meeting 1april, we said that I should have data_array.buffer as the input here but then I just get the error:  'numpy.ndarray' object has no attribute 'buffer'
            print(f'Sent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}')
        elif compr == 'none':
            det_socket.send_json(dct, flags=zmq.SNDMORE)
            det_socket.send(data_array)  # in the meeting 1aprildet, we said that I should have data_array.buffer as the input here but then I just get the error:  'numpy.ndarray' object has no attribute 'buffer'
            print(f'\nSent frame nr. {i} at time {time.strftime("%H:%M:%S", time.localtime())}')


        if i == nframes - 1:
            if not sleeptime == 0: time.sleep(sleeptime)
            i += 1
            k += 1
            dct_last = {'htype': 'series_end', 'msg_number': k}
            det_socket.send_json(dct_last)
            print(f'Finished at time {time.strftime("%H:%M:%S", time.localtime())}, sending all messages took {time.time()-t1} s.')
            break
    except KeyboardInterrupt:
        print(f'i = {i}\nKeyboardInterrupt at time {time.strftime("%H:%M:%S", time.localtime())}\n')
        det_socket.close()
        break
det_socket.close()
