"""
Run a specific test with prints to stdout:
py.test -v --capture=tee-sys /home/reblex/RelayServer/test_relayserver.py::test_run_recon

Or specific tests who's name contains the string "test_run_recon2_":
py.test -k "test_run_recon2_" -v --capture=tee-sys /home/reblex/RelayServer/test_relayserver.py

--capture=fd                            shows all output
--capture=sys                           shows rec output
--capture=tee-sys                       shows rec output
--capture=no                            shows rec output
--capture=sys --show-capture=no         shows no output
--capture=no --show-capture=no          shows rec output
--show-capture=no                       shows rs output
--show-capture=stderr                   shows rs output
--capture=sys --show-capture=no         shows


  --capture=method      per-test capturing method: one of fd|sys|no|tee-sys.
  --show-capture={no,stdout,stderr,log,all}
                        Controls how captured stdout/stderr/log is shown on failed tests. Default is 'all'.
"""

import os
try:
    from . import relayserver
    from .relayserver import RelayServer
except:
    ### DEBUG: only used in the development stage in order to run things in ipython
    from relayserver import RelayServer
    import relayserver
import numpy as np
import pytest
import sys
import subprocess
import time
import threading
from threading import Thread
from threading import local
import contextlib
import io
from io import StringIO
import inspect
"""
Ideas for tests to implement:
* Check that correct decompression is used, i.e. that it includes/excludes the first 12 bytes.
* Check that RS receives messages from both Motor_streamer and Detector_streamer in simulation mode.
* Check that rebinning is done as expected.
* When running together with ptypy, check that: 
    * RS receives a 'preprocess' request.
    * RS receives a 'check_energy' request.
    X all sockets are closed after ptypy knows the scan has ended.
    
+ Implement something like 'if ptypy is installed then also do the full reconstruction tests.'

"""
print('Starting tests')

class ThreadWithReturnValue(Thread):
    """
    Copied from https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread
    """
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                        **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


def modify_code(function, add_code, index=None):
    """

    Parameters
    ----------
    function
    add_code  : type {str}
    index  :  line number where 'add_code' should be inserted.
                Default: -2, if last line of function includes 'return', -1 otherwise.

    Returns
    -------
    new_func  :  A string of 'function'-s source code with 'add_code' inserted at the
                end of the function, that can the executed using exec(new_func).

    """
    # func_str = inspect.getsource(function)
    # func_name = function.__name__ + '()' ## slower alternative: func_name = func_str.split(":\n")[0].removeprefix("def ")
    # new_func = func_str

    func_strings = inspect.getsourcelines(function)[0]
    func_name = function.__name__ + '()' ## slower alternative: func_name = func_strings[0].split(":\n")[0].removeprefix("def ")
    if index is None:
        if 'return' in func_strings[-1]:
            index = -2
        else:
            index = -1

    if not add_code.endswith('\n'):
        add_code += '\n'

    if func_strings[index].startswith('    '):
        nr_ws = func_strings[index].count('    ')
        if not add_code.startswith(func_strings[index].count('    ')*'    '):
            print("\nNOTE: The added code has a different indentation level than where it's inserted!\n")

    func_strings.insert(index, str(add_code))
    new_func = ''
    new_func = new_func.join(func_strings)
    new_func += func_name

    return new_func


# @pytest.fixture(params=[[None, 1],
#                         [10, 1]], scope='session'
#                 )
def reconstruct(frames_per_iter):###request):
    import sys
    import time
    import socket
    import ptypy
    from ptypy.core import Ptycho
    from ptypy import utils as u
    from mpi4py import MPI


    if float(ptypy.version[:3]) >= float('0.5'):
        ptypy.load_ptyscan_module("livescan")

    ### frames_per_iter, numiter = request.param
    scannr = 0
    defocus_um = 600

    # General parameters
    p = u.Param()
    p.verbose_level = 0#'interactive'
    p.run = 'scan%d' % scannr

    p.io = u.Param()
    p.io.interaction = u.Param()
    p.io.interaction.active = False
    # p.io.interaction.server = u.Param()
    # IPaddr = socket.gethostbyname(socket.gethostname())
    # p.io.interaction.server.address = f'tcp://{IPaddr}'
    # p.io.interaction.server.port = 5560
    # p.io.interaction.client = u.Param()
    # p.io.interaction.client.address = f'tcp://{IPaddr}'
    # p.io.interaction.client.port = 5560
    p.io.autosave = u.Param()
    p.io.autosave.active = False
    p.io.autoplot = u.Param()
    p.io.autoplot.active = False

    # Scan parameters
    p.scans = u.Param()
    p.scans.scan00 = u.Param()
    p.scans.scan00.name = 'BlockFull'
    p.scans.scan00.data = u.Param()
    p.scans.scan00.data.name = 'LiveScan'
    p.scans.scan00.data.detector = 'eiger'
    p.scans.scan00.data.xMotor = 'sx'
    p.scans.scan00.data.yMotor = 'sy'
    p.scans.scan00.data.relay_host = 'tcp://127.0.0.1'
    p.scans.scan00.data.relay_port = 45678
    p.scans.scan00.data.shape = (256, 256)
    p.scans.scan00.data.crop_at_RS = (256, 256)
    p.scans.scan00.data.rebin = None
    p.scans.scan00.data.rebin_at_RS = 4
    p.scans.scan00.data.center = None
    p.scans.scan00.data.auto_center = None
    p.scans.scan00.data.xMotorFlipped = False
    p.scans.scan00.data.yMotorFlipped = False
    p.scans.scan00.data.orientation = None
    p.scans.scan00.data.distance = 3.512
    p.scans.scan00.data.psize = 75e-6
    p.scans.scan00.data.min_frames = 1
    p.scans.scan00.data.block_wait_count = 1
    p.scans.scan00.data.start_frame = 1
    p.scans.scan00.data.frames_per_iter = frames_per_iter #10#None
    p.scans.scan00.data.load_parallel = 'all'

    # Scan parameters: illumination
    p.scans.scan00.illumination = u.Param()
    p.scans.scan00.illumination.model = None
    p.scans.scan00.illumination.aperture = u.Param()
    p.scans.scan00.illumination.aperture.form = 'rect'
    p.scans.scan00.illumination.aperture.size = 100e-9
    p.scans.scan00.illumination.propagation = u.Param()
    p.scans.scan00.illumination.propagation.parallel = -1. * defocus_um * 1e-6
    p.scans.scan00.illumination.diversity = u.Param()
    p.scans.scan00.illumination.diversity.noise = (.5, 1.0)
    p.scans.scan00.illumination.diversity.power = .1

    # Engine
    p.engines = u.Param()
    p.engines.engine00 = u.Param()
    p.engines.engine00.name = 'DM'
    p.engines.engine00.numiter = 1 ### numiter#1
    p.engines.engine00.numiter_contiguous = 1
    p.engines.engine00.probe_support = 10

    p.frames_per_block = 1

    P = Ptycho(p, level=5)
    return p, P

#%%

##############################################################################
@pytest.fixture(params=[[(10)], [None]], scope='session')  # function, class, module, package or session
def run_recon3(request):### reconstruct):
    print('Starting run_recon2'.center(80, '/'))
    frames_per_iter = request.param
    rs = RelayServer()
    thread_rec = ThreadWithReturnValue(target=reconstruct, args=frames_per_iter)
    thread_rs = threading.Thread(target=relayserver.launch, args=(rs,))
    stdout = StringIO()
    sys.stdout = stdout  # suppresses output, stopped working for some reason..
    thread_rec.start()
    thread_rs.start()
    while thread_rec.is_alive():
        time.sleep(1)
    sys.stdout = sys.__stdout__  # stops suppressing output
    print(f'\n\nis_alive(thread_rs, thread_rec): {thread_rs.is_alive()}, {thread_rec.is_alive()}\n')
    recout = thread_rec.join(1)
    thread_rs.join(1)
    if thread_rec._tstate_lock is not None:
        thread_rec._tstate_lock.release()
        print('Releasing thread_rec._tstate_lock')
    if thread_rs._tstate_lock is not None:
        thread_rs._tstate_lock.release()
        print('Releasing thread_rs._tstate_lock')
    output = stdout.getvalue()
    print('Ending run_recon2'.center(80, '/'))
    return thread_rs, thread_rec, recout, stdout, output, rs

def test_run_recon3_1(run_recon3):
    """
    Make sure that that RelayServer terminated properly
    WORKS!!
    """
    print('Starting test_run_recon2_1'.center(80, '\\'))
    thread_rs, thread_rec, recout, stdout, output, rs = run_recon3
    print('running', rs.running)
    # if len(rs) <= 0:
    #     pytest.fail("No parameters saved from RS.")
    sys.stderr.write(f"test socket type: {type(rs.relay_socket)}\n")
    print('relay_socket.closed :  ', rs.relay_socket.closed)
    if not rs.relay_socket.closed:
        rs.stop_outstream()
        print('relay_socket.closed 2:  ', rs.relay_socket.closed)
        pytest.fail("relay_socket not closed.")
    print('Ending test_run_recon3'.center(80, '\\'))
##############################################################################


#%%

def test_run_recon3_2(run_recon3):
    """
    Makes sure that diffraction patterns used in the reconstruction
    are the same as the ones sent from the RelayServer.
    """
    thread_rs, thread_rec, recout, stdout, output, rs = run_recon3
    if len(rs.sendimg) != len(recout[1].diff.S.keys()):
        print(f"len(rs[0].sendimg) = {len(rs.sendimg)}")
        print(f"len(recout[1].diff.S.keys()) = {len(recout[1].diff.S.keys())}")
        print(f"rs[0].sendimg[0].shape[-3] = {rs.sendimg[0].shape[-3]}")
        pytest.fail(f"Mismatch in nr of image-chunks: {len(rs.sendimg)} vs. {len(recout[1].diff.S.keys())}.")
    for chunk in zip(range(len(rs.sendimg)), recout[1].diff.S.keys()):
        assert (rs.sendimg[chunk[0]][0] == recout[1].diff.S[chunk[1]].data).all()


def test_run_recon3_3(run_recon3):
    """
    Makes sure that RS receives a 'preprocess' and a 'check_energy' request.
    """
    thread_rs, thread_rec, recout, stdout, output, rs = run_recon3
    if not rs.energy_replied:
        pytest.fail("Energy was never sent by RS.")
    if len(rs.init_params) <= 0:
        pytest.fail("RS never received a preprocess request.")
    #assert rsout.sendimg[0][0].shape == recout[1].diff.S['S0000'].data.shape

#%%
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


# %% try creating the test using fixture

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


# %%
#
# def test_recon():
#     print('\n------------------ test_recon() ------------------')
#     recon = subprocess.Popen([sys.executable, '/home/reblex/Documents/Scripts/Reconstruct_livescan_siemens_KB.py'],
#                              stdout=subprocess.PIPE,
#                              # stdout='/home/reblex/Desktop/temp.log',
#                              # shell=True,
#                              # universal_newlines=True,
#                              stderr=subprocess.STDOUT)
#     print(f'Started reconstruction in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')
#     # time.sleep(10)
#     output = recon.communicate()[0].decode().split('\n')
#     print(output)
#     # recon.wait(10)
#
#
# def test_RS():
#     """ RS, recon =  test_RS()
#     Test for making sure that the RelayServer closes the connection
#     to PtyPy when PtyPy has registered that all frames have been received.
#     Note!
#         Running the reconstruction with pytest takes a lot longer than normally,
#         e.g. one reconstruction that takes 17 s normally took 61 s with pytest.
#
#     This test does not work yet, since the process will get stuck in RS.run()
#     if RS.relay_socket.closed = False...
#
#     Possible solutions to this:
#         * make a timeout for how long RS.run has been running and if it takes too long then make the test fail
#         * see how to check if a subprocess has exited, and put RS.run into a separate process as well,
#         then if recon has exited but not RS.run then fail the test!
#     """
#     print('\n------------------ test_RS() ------------------')
#     recon = subprocess.Popen([sys.executable, '/home/reblex/Documents/Scripts/Reconstruct_livescan_siemens_KB.py'],
#                              stdout=subprocess.PIPE,
#                              stderr=subprocess.STDOUT)
#     print(f'Started reconstruction in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')
#
#     # RS = RelayServer()
#     # RS.connect(detector_address='tcp://0.0.0.0:56789', motors_address='tcp://127.0.0.1:5556', relay_address='tcp://127.0.0.1:45678', simulate=True)
#
#     print(os.path.dirname(os.path.abspath(__file__)))
#     RSrun = subprocess.Popen([sys.executable, "-c",
#                               "import os;"
#                               f"os.chdir(os.path.dirname(f'{os.path.abspath(__file__)}'));"
#                               "from relayserver import RelayServer;"
#                               "RS = RelayServer();"
#                               "RS.connect(detector_address='tcp://0.0.0.0:56789', motors_address='tcp://127.0.0.1:5556', relay_address='tcp://127.0.0.1:45678', simulate=True);"
#                               "RS.run()"
#                               ],
#                              stdout=subprocess.PIPE,
#                              stderr=subprocess.STDOUT)
#     print(f'Started RelayServer in subprocess at {time.strftime("%H:%M:%S", time.localtime())}')
#
#     recpoll = None
#     rspoll = None
#     t0 = time.time()
#     while recpoll is None and time.time() - t0 < 10:
#         time.sleep(2)
#         recpoll = recon.poll()
#         rspoll = RSrun.poll()
#         print(f'{(time.time() - t0):.04}:  recon.poll = {recpoll}, RSrun.poll = {rspoll}')
#
#     # recon.wait(20)
#     # print(recon.communicate()[0].decode().split('\n'))
#     # print(RSrun.communicate()[0].decode().split('\n'))
#     #
#     #
#     assert recon.returncode == 0
#     if recon.returncode == 0:
#         assert RSrun.returncode == 0
#     # # if 'End of scan reached' in recon.communicate()[0].decode().split('\n'):
#     # #     assert RS.relay_socket.closed
#     return recon, RSrun
#
#
# #recproc, rsproc = test_RS()
#
# # %%
#
