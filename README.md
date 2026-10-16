# RelayServer

Intended to be used together with 
https://github.com/ptycho/ptypy/tree/livescan_maxiv  

Example usage of the RelayServer for simulating an ongoing experiment:

$ input='{"sleeptime": "0.7125", "scan_file": "/your/path/scan_000026_eiger1m.hdf5", "path_to_data": "/entry/instrument/Eiger/data", "detector": "eiger1m"}'
<br>
$ python RelayServer/relayserver.py "${input}"

<br><br>
<img src="https://github.com/user-attachments/assets/e95de032-6107-4bd8-911c-86e001fef0a0" alt="Streaming archetecture" width="700"/>

[//]: #![Relay_overview](https://github.com/user-attachments/assets/e95de032-6107-4bd8-911c-86e001fef0a0)

Tested using Python 3.8.6.
