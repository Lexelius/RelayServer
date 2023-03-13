from RelayServer import RelayServer as RS
import numpy as np
import pytest

print('Starting tests')

diff = np.ones((10, 7, 5)) * np.arange(1, 36).reshape((7, 5))



# @pytest.mark.parametrize("diff, get_weights, expected", [
#     (np.arange(-2, 3), True, 'expected'),
#     (np.arange(-2, 3), False, 'expected')
# ])
@pytest.mark.parametrize("diff, get_weights", [
    (diff, True),
    (diff, False)
])
def test_crop(diff, get_weights): #, expected):
    # return diff_croppad, c_a, padwidth
    # self.init_params['shape']
    # self.init_params['center']
    # self.center
    RS.init_params['shape'] = (7, 7)
    RS.init_params['center'] = (2, 1)
    diff[:, RS.init_params['center']] = 1000
    diff_croppad, c_a, padwidth = RS.crop(diff, get_weights=False)
    assert diff_croppad[c_a] == 1000
    #assert (RS.crop(diff, get_weights=False) == expected).all()



#%% Old test function I made
"""
def croptst( diff, cropshape, center):

    # [cropshape] type: int, tuple
    # [center] type: np.array()

    # a: desired crop shape, b: resulting crop shape, c: center , d: diff shape
    cropshape = np.min(cropshape)
    c_d = center

    # Find limits for cropping
    d_sh = diff.shape[-2:]  # (rows, cols) (yspan, xspan)
    a_sh = int(cropshape)

    a_lowlims = c_d - a_sh // 2  # !##  array([367, -41])
    a_highlims = c_d + (a_sh + 1) // 2  # (a_sh + 1) to enable uneven nr of pixels.

    # Set the limits within the boundaries of diffraction pattern
    b_lowlims = np.max((a_lowlims, np.zeros(2, dtype='int')), axis=0)
    b_highlims = np.min((a_highlims, np.array(d_sh)), axis=0)
    lims = np.array([b_lowlims, b_highlims])

    ##diff_crop = diff[b_lowlims[0]:b_highlims[0], b_lowlims[1]:b_highlims[1]]
    diff_crop = np.ascontiguousarray(np.split(np.split(diff, lims[:, 0], axis=-2)[1], lims[:, 1], axis=-1)[1])

    ##c_a = np.array([a_sh//2, a_sh//2])
    c_b = tuple(c_d - b_lowlims)  # center w.r.t cropped diffraction pattern

    ## adding padding
    padlims = np.array([b_lowlims - a_lowlims, a_highlims - b_highlims])
    padwidth = (padlims[:, 0], padlims[:, 1]) ## {sequence, array_like, int}

    padwidth = np.zeros((len(diff.shape),2), dtype='int64') # Used to get the correct shape for padwidth in np.pad
    #padwidth[-2:,:] = np.array([b_lowlims - a_lowlims, a_highlims - b_highlims])
    padwidth[-2:, :] = np.array([(padlims[:, 0], padlims[:, 1])])
    diff_croppad = np.pad(diff_crop, padwidth)

    return diff_croppad, diff_crop, c_b, c_d, d_sh, a_sh, a_lowlims, a_highlims, b_lowlims, b_highlims, lims, padlims

from tabulate import tabulate
centers = [(2,1), (1,2), (0,2), (2,3), (3,3), (3,1), (4,1), (6,2), (5,2), (4,3)]
shapes = [(7,7), (5,5), (3,3), (7,7), (5,5), (5,5), (7,7), (3,3), (5,5), (7,7)]
for k in range(len(centers)):
    c1 = centers[k]
    A1 = np.arange(35).reshape((7,5))
    A1[c1] = 1000
    A1 = np.array([A1, A1, A1, A1, A1, A1])
    diff_croppad, diff_crop, c_b, c_d, d_sh, a_sh, a_lowlims, a_highlims, b_lowlims, b_highlims, lims, padlims = croptst(A1, shapes[k], np.array(c1))
    print(tabulate([[k, '\tcenter: ', c1, ', shape: ', shapes[k], 'blims: ', lims, 'alims: ', np.array([a_lowlims, a_highlims]), 'padlims: ', padlims]]))
    print(diff_croppad, '\n')
    #print(k, 'center: ', c1, ', shape: ', shapes[k], 'diff_croppad: \n', diff_croppad, '\n\n')
    #print('A1:\n', A1, '\ndiff_crop:\n', diff_crop, '\ndiff_croppad:\n', diff_croppad)
##diff_croppad, diff_crop, c_b, c_d, d_sh, a_sh, a_lowlims, a_highlims, b_lowlims, b_highlims, lims, padlims = croptst(RS.all_img[0], self.init_params['shape'], self.center)
"""