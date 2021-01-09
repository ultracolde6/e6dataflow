import numpy as np


def integrate_roi(img, roi_slice):
    return np.nansum(img[roi_slice])