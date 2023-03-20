"""
Run with prints to stdout:
py.test -v --capture=tee-sys /home/reblex/RelayServer/test_RelayServer.py
"""
from RelayServer import RelayServer
import numpy as np
import pytest
import sys
import subprocess
import time

print('Starting tests')


@pytest.mark.parametrize("diff, get_weights", [
    (np.ones((10, 7, 5)) * np.arange(1, 36).reshape((7, 5)), False)
    ])
def test_crop(diff, get_weights):
    RS = RelayServer()
    RS.init_params['shape'] = (7, 7)
    RS.init_params['center'] = (2, 1)
    diff[:, RS.init_params['center'][0], RS.init_params['center'][1]] = 1000
    diff_croppad, c_a, padwidth = RelayServer.crop(RS, diff, get_weights=get_weights)
    assert (diff_croppad[:, c_a[0], c_a[1]] == 1000).all()


#%% try creating the test using fixture

@pytest.fixture
def RS_instance():
    RS = RelayServer()
    RS.init_params['shape'] = (7, 7)
    RS.init_params['center'] = (2, 1)
    return RS


@pytest.fixture(params=[[(7, 7), (2, 1)],
                        [(5, 5), (1, 2)],
                        [(3, 3), (0, 2)],
                        [(7, 7), (2, 3)],
                        [(5, 5), (3, 3)],
                        [(5, 5), (3, 1)],
                        [(7, 7), (4, 1)],
                        [(3, 3), (6, 2)],
                        [(5, 5), (5, 2)],
                        [(7, 7), (4, 3)]]
                )
def RS_diff_opts(request):
    print('\n', '_'.ljust(70, '_'))
    sh, cen = request.param
    RS = RelayServer()
    RS.init_params['shape'] = sh
    RS.init_params['center'] = cen
    diff = np.ones((10, 7, 5)) * np.arange(1, 36).reshape((7, 5))
    print(f'\nRS_diff_opts: \t\t sh = {sh},\t cen = {cen}')
    print('\ndiff[0] 1: \n', diff[0])
    diff[:, RS.init_params['center'][0], RS.init_params['center'][1]] = 1000
    print('\ndiff[0] 2: \n', diff[0])
    return RS, diff


@pytest.mark.parametrize("get_weights", [False, True])
def test_crop2(RS_diff_opts, get_weights):
    print('\n', '-'.ljust(70, '-'), '\n')
    RS, diff = RS_diff_opts
    diff_croppad, c_a, padwidth = RS.crop(diff, get_weights)
    print(f"diff_croppad.shape = {diff_croppad.shape}\nc_a = {c_a},\ttype(c_a) = {type(c_a)}\nget_weights = {get_weights}")
    print(diff_croppad[:, c_a[0], c_a[1]])
    print(f"\ndiff[0] = \n {diff[0]}\n\n")
    print(f"diff_croppad[0] = \n {diff_croppad[0]}\n\n")
    assert (diff_croppad[:, c_a[0], c_a[1]] == 1000).all()


#%%
def test_recon():
    print('\n------------------ test_recon() ------------------')
    recon = subprocess.Popen([sys.executable, '/home/reblex/Documents/Scripts/Reconstruct_livescan_siemens_KB.py'],
                             stdout=subprocess.PIPE,
                             #stdout='/home/reblex/Desktop/temp.log',
                             # shell=True,
                             # universal_newlines=True,
                             stderr=subprocess.STDOUT)
    print(f'Started reconstruction in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')
    # time.sleep(10)
    output = recon.communicate()[0].decode().split('\n')
    print(output)
    # recon.wait(10)


def test_RS():
    """ RS, recon =  test_RS()
    Test for making sure that the RelayServer closes the connection
    to PtyPy when PtyPy has registered that all frames have been received.

    This test does not work yet, since the process will get stuck in RS.run()
    if RS.relay_socket.closed = False...

    Possible solutions to this:
        * make a timeout for how long RS.run has been running and if it takes too long then make the test fail
        * see how to check if a subprocess has exited, and put RS.run into a separate process as well,
        then if recon has exited but not RS.run then fail the test!
    """
    print('\n------------------ test_RS() ------------------')
    recon = subprocess.Popen([sys.executable, '/home/reblex/Documents/Scripts/Reconstruct_livescan_siemens_KB.py'],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT,
                     check=True)
    print(f'Started reconstruction in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')

    import os
    # RS = RelayServer()
    # RS.connect(detector_address='tcp://0.0.0.0:56789', motors_address='tcp://127.0.0.1:5556', relay_address='tcp://127.0.0.1:45678', simulate=True)

    RSrun = subprocess.Popen([sys.executable, "-c",
                         "import os;"
                         f"os.chdir(os.path.dirname(f'{os.path.abspath(__file__)}'));"
                         "from RelayServer import RelayServer;"
                         "RS = RelayServer();"
                         "RS.connect(detector_address='tcp://0.0.0.0:56789', motors_address='tcp://127.0.0.1:5556', relay_address='tcp://127.0.0.1:45678', simulate=True);"
                         "RS.run()"
                         ],
                 stdout=subprocess.PIPE,
                 stderr=subprocess.STDOUT,
                 check=True)
    print(f'Started RelayServer in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')

    # retcode = None
    # while retcode is None:
    #     time.sleep(0.5)
    #     retcode = recon.poll()
    #     print(retcode, recon.communicate()[0].decode().split('\n'))
    recon.wait(40)
    print(recon.communicate()[0].decode().split('\n'))
    print(RSrun.communicate()[0].decode().split('\n'))
    print(os.path.dirname(os.path.abspath(__file__)))

    assert recon.returncode == 0
    if recon.returncode == 0:
        assert RSrun.returncode == 0
    # if 'End of scan reached' in recon.communicate()[0].decode().split('\n'):
    #     assert RS.relay_socket.closed



#%%