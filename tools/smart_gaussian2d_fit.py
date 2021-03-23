import numpy as np
import scipy.ndimage
import scipy.stats
from scipy.optimize import least_squares
from scipy.special import erf
import time
from uncertainties import ufloat
import matplotlib.pyplot as plt


def gaussian_2d(x, y, x0=0, y0=0, sx=1, sy=1, amp=1, offset=0, angle=0, x_slope=0, y_slope=0):
    angle_rad = np.radians(angle)
    rx = np.cos(angle_rad) * (x - x0) + np.sin(angle_rad) * (y - y0)
    ry = -np.sin(angle_rad) * (x - x0) + np.cos(angle_rad) * (y - y0)
    return (amp * np.exp(-(1 / 2) * ((rx / sx) ** 2 + ((ry / sy) ** 2)))
            + offset + x_slope * (x - x0) + y_slope * (y - y0))


def img_moments(img):
    y_inds, x_inds = np.indices(img.shape)
    tot = np.nansum(img)
    if tot <= 0:
        raise ValueError('Integrated image intensity is negative, image may be too noisy. '
                         'Image statistics cannot be calculated.')
    x0 = np.nansum(img*x_inds)/tot
    y0 = np.nansum(img*y_inds)/tot
    varx = np.nansum(img * (x_inds - x0)**2) / tot
    vary = np.nansum(img * (y_inds - y0)**2) / tot
    if varx <= 0 or vary <= 0:
        raise ValueError('varx or vary is negative, image may be too noisy. '
                         'Image statistics cannot be calculated.')
    sx = np.sqrt(varx)
    sy = np.sqrt(vary)
    return x0, y0, sx, sy


def get_guess_values(img, quiet=True):
    x_range = img.shape[1]
    y_range = img.shape[0]
    amp_guess = np.nanmax(img) - np.nanmin(img)
    offset_guess = np.nanmin(img)
    try:
        x0_guess, y0_guess, sx_guess, sy_guess = img_moments(img)
    except ValueError as e:
        if not quiet:
            print(e)
            print('Using default guess values.')
        x0_guess, y0_guess, sx_guess, sy_guess = [x_range/2, y_range/2, x_range/2, y_range/2]
    p_guess = np.array([x0_guess, y0_guess, sx_guess, sy_guess, amp_guess, offset_guess])
    if not quiet:
        print(f'x0_guess = {x0_guess:.1f}')
        print(f'y0_guess = {y0_guess:.1f}')
        print(f'sx_guess = {sx_guess:.1f}')
        print(f'sy_guess = {sy_guess:.1f}')
    return p_guess


def make_fit_param_dict(name, val, std, conf_level=erf(1 / np.sqrt(2)), dof=None):
    pdict = {'name': name, 'val': val, 'std': std, 'conf_level': conf_level}
    if dof is None:  # Assume normal distribution if dof not specified
        tcrit = scipy.stats.norm.ppf((1 + conf_level) / 2)
    else:
        tcrit = scipy.stats.t.ppf((1 + conf_level) / 2, dof)
    pdict['err_half_range'] = tcrit * std
    pdict['err_full_range'] = 2 * pdict['err_half_range']
    pdict['val_lb'] = val - pdict['err_half_range']
    pdict['val_ub'] = val + pdict['err_half_range']
    return pdict


def make_visualization_figure(fit_struct, show_plot=True, save_name=None):
    # TODO: Catch error if center of fit is outside plot range
    img = fit_struct['data_img']
    model_img = fit_struct['model_img']
    x_range = img.shape[1]
    y_range = img.shape[0]
    x0 = int(round(fit_struct['x0']['val']))
    y0 = int(round(fit_struct['y0']['val']))
    sx = fit_struct['sx']['val']
    sy = fit_struct['sy']['val']
    img_min = np.min([img.min(), model_img.min()])
    img_max = np.max([img.max(), model_img.max()])

    # Plotting
    fig = plt.figure(figsize=(8, 8))

    # Data 2D Plot
    ax_data = fig.add_subplot(2, 2, 1, position=[0.1, 0.5, 0.25, 0.35])
    ax_data.imshow(img, vmin=img_min, vmax=img_max, cmap='binary_r')
    # TODO: check aspect ratio
    ax_data.set_aspect(y_range / x_range)
    ax_data.xaxis.tick_top()
    ax_data.set_xlabel('Horizontal Position')
    ax_data.xaxis.set_label_position('top')
    ax_data.set_ylabel('Vertical Position')
    ax_data.set_ylim(0, y_range)
    ax_data.set_xlim(0, x_range)

    # Fit 2D Plot
    ax_fit = fig.add_subplot(2, 2, 4, position=[0.4, 0.1, 0.25, 0.35])
    ax_fit.imshow(model_img, vmin=img_min, vmax=img_max, cmap='binary_r')
    ax_fit.set_aspect(y_range / x_range)
    ax_fit.yaxis.tick_right()
    ax_fit.set_xlabel('Horizontal Position')
    ax_fit.set_ylabel('Vertical Position')
    ax_fit.yaxis.set_label_position('right')
    ax_fit.set_ylim(0, y_range)
    ax_fit.set_xlim(0, x_range)

    # Y Linecut Plot
    ax_yline = fig.add_subplot(2, 2, 2, position=[0.4, 0.5, 0.25, 0.35])
    y_int_cut_dat = np.sum(img, axis=1) / np.sqrt(2 * np.pi * sx**2)
    y_int_cut_model = np.sum(model_img, axis=1) / np.sqrt(2 * np.pi * sx**2)
    ax_yline.plot(y_int_cut_dat, range(y_range), 'o', zorder=1)
    ax_yline.plot(y_int_cut_model, range(y_range), zorder=2)
    ax_yline.yaxis.tick_right()
    ax_yline.xaxis.tick_top()
    ax_yline.set_xlabel('Integrated Intensity')
    ax_yline.xaxis.set_label_position('top')
    ax_data.axvline(x0, linestyle='--')
    ax_fit.axvline(x0, linestyle='--')

    # X Linecut Plot
    ax_xline = fig.add_subplot(2, 2, 3, position=[0.1, 0.1, 0.25, 0.35])
    x_int_cut_dat = np.sum(img, axis=0) / np.sqrt(2 * np.pi * sy**2)
    x_int_cut_model = np.sum(model_img, axis=0) / np.sqrt(2 * np.pi * sy**2)
    ax_xline.plot(range(x_range), x_int_cut_dat, 'o', zorder=1)
    ax_xline.plot(range(x_range), x_int_cut_model, zorder=2)
    ax_xline.invert_yaxis()
    ax_xline.set_ylabel('Integrated Intensity')
    ax_data.axhline(y0, linestyle='--')
    ax_fit.axhline(y0, linestyle='--')

    print_str = ''
    for key in fit_struct['param_keys']:
        param = fit_struct[key]
        val = round(param['val'], 3)
        std = round(param['std'], 3)
        print_str += f"{key} = {ufloat(val, std)}\n"
    fig.text(.8, .5, print_str)

    fit_struct['fit_fig'] = fig

    if save_name is not None:
        plt.savefig(save_name)
    if show_plot:
        plt.show()
    else:
        plt.close()

    return


def create_fit_struct(img, popt_dict, pcov, conf_level, dof, lightweight=False):
    y_coords, x_coords = np.indices(img.shape)
    model_img = gaussian_2d(x_coords, y_coords, **popt_dict)
    fit_struct = dict()
    fit_struct_param_keys = []
    for i, key in enumerate(popt_dict.keys()):
        fit_param_dict = make_fit_param_dict(key, popt_dict[key], np.sqrt(pcov[i, i]), conf_level, dof)
        fit_struct[key] = fit_param_dict
        fit_struct_param_keys.append(key)
    fit_struct['param_keys'] = fit_struct_param_keys
    fit_struct['cov'] = pcov
    if not lightweight:
        fit_struct['data_img'] = img
        fit_struct['model_img'] = model_img
    fit_struct['NGauss'] = fit_struct['amp']['val'] * 2 * np.pi * fit_struct['sx']['val'] * fit_struct['sy']['val']
    fit_struct['NSum'] = np.sum(img)
    # TODO: NSum_BGsubtract should subtract linear background as well if it was fitted for
    fit_struct['NSum_BGsubtract'] = np.sum(img - fit_struct['offset']['val'])
    return fit_struct


# noinspection PyTypeChecker
def fit_gaussian2d(img, zoom=1.0, angle_offset=0.0, fix_lin_slope=False, fix_angle=False,
                   show_plot=True, save_name=None, conf_level=erf(1 / np.sqrt(2)), quiet=True, lightweight=False,
                   guess=None):
    """
    2D Gaussian fit to an image
    Guassian fitting algorithm operates by taking an input image img, extracting a guess for initial fit parameters
    and then performing a Gaussian fit. The initial guess is either based on the mean and variance of img or the image
    size. There are options to fit or constrain the tilt angle of the ellipse and a 2D linear sloping background.
    Returns a fit_struct dictionary object which contains some detailed information about the fit including the
    fit value, standard deviation, and confidence intervals for all fit parameters.
    :param img: 2D Image to fit
    :param zoom: Decimate rate to speed up fitting if downsample is selected
    :param angle_offset: (degrees) Central value about which tilt angle is expected to scatter. Output values for
                         angle will be +- 45 deg. Fits with tilt angle near the edge of this range may swap sx and sy
                         for similar looking images
    :param fix_lin_slope: Flag to indicate if a fit should constrain linear background to zero
    :param fix_angle: Flag to indicate if fit should constrain tilt angle to zero degrees
    :param show_plot: Flag to indicate whether to show fit visualization
    :param save_name: File name for saved figure, default None value means don't save figure
    :param conf_level: Confidence level for confidence intervals
    :param quiet: Squelch variable
    :param lightweight: If False then the data image and model fit image are saved into the fit_struct.
                        For large images this might make the fit_struct have a large size in memory.
    :param guess: Optional guess values for parameters
    :return fit_struct: Returns a struct containing relevant data output of the fit routine
    :rtype dict
    Returns
    _______
    `fit_struct` dictionary with the following keys defined:
    x0 : dict
        fit_param_dict for fit center x-coordinate
    y0 : dict
        fit_param_dict for fit center y-coordinate
    sx : dict
        fit_param_dict for fit standard deviation in x-coordinate
    sy : dict
        fit_param_dict for fit standard deviation in y-coordinate
    A : dict
        fit_param_dict for fit amplitude
    offset : dict
        fit_param_dict for background offset
    theta (optionally) : dict
        fit_param dict for tilt angle (degrees). Constrained to angle_offset +- 45 deg
        enabled if fix_tilt_angle=False
    x_slope (optionally) : dict
        fit_param_dict for linear slope in x-coordinate. enabled if fix_lin_slope=False
    y_slope (optionally) : dict
        fit_param_dict for linear slope in y-coordinate. enabled if fix_lin_slope=False
    cov : ndarray
        Covariance matrix estimated from fit Jacobian
    data_img: ndarray
        Copy of input image, img
    model_img: ndarray
        Image representing the best fit to img using 2D Gaussian model
    NGauss: float
        Total area under Gaussian fit model (assuming infinite range, not restricted to image area even if Gaussian
        variance is much larger than image size
    NSum: float
        Integrated sum of all pixel values in original image
    NSum_BGsubtract: float
        Subtract off fitted background from NSum: NSum - offset. NSum - offset.
    fit_param_dict
    _______
    fit_param_dict are dictionaries carries information about individual fit parameters with the following keys
    defined:
    name : str
        parameter name
    val : float
        central fit value for parameter
    std : float
        standard deviation of fit extracted from fit Jacobian matrix
    conf_level : float
        confidence level for fit parameter confidence interval
    err_half_range : float
        Half the size of the confidence interval
    err_full_range : float
        Full size of the confidence interval
    val_lb : float
        Lower limit of confidence interval
    val_ub : float
        Upper limit of confidence interval
    """

    img_downsampled = scipy.ndimage.interpolation.zoom(img, 1 / zoom)
    # zoom = 1.0
    img_downsampled = np.nan_to_num(img)
    img = np.nan_to_num(img)
    if not quiet:
        print(f'Image downsampled by factor: {zoom:.1f}')
    y_coords, x_coords = np.indices(img_downsampled.shape)

    if guess is None:
        p_guess = get_guess_values(img, quiet=quiet)
    else:
        p_guess = guess
    param_keys = ['x0', 'y0', 'sx', 'sy', 'amp', 'offset']
    lock_params = dict()
    if fix_angle:
        lock_params['angle'] = 0
    else:
        param_keys.append('angle')
        p_guess = np.append(p_guess, 0)
    if fix_lin_slope:
        lock_params['x_slope'] = 0
        lock_params['y_slope'] = 0
    else:
        param_keys.extend(['x_slope', 'y_slope'])
        p_guess = np.append(p_guess, [0, 0])

    def img_cost_func(x):
        return np.nan_to_num(np.ravel(gaussian_2d(x_coords * zoom, y_coords * zoom,
                                                  *x, **lock_params)
                             - img_downsampled))
    t_fit_start = time.time()
    lsq_struct = least_squares(img_cost_func, p_guess, verbose=0)
    t_fit_stop = time.time()
    if not quiet:
        print(f'fit time = {t_fit_stop - t_fit_start:.2f} s')

    popt = lsq_struct['x']
    popt_dict = dict(zip(param_keys, popt))
    jac = lsq_struct['jac']
    cost = lsq_struct['cost']

    popt_dict['sx'] = np.abs(popt_dict['sx'])
    popt_dict['sy'] = np.abs(popt_dict['sy'])

    if not fix_angle:
        angle = popt_dict['angle']
        angle_diff = (angle - angle_offset) % 360
        if 0 <= angle_diff < 45:
            angle = angle_offset + angle_diff
        elif 45 <= angle_diff < 135:
            angle = angle_offset + angle_diff - 90
            popt_dict['sx'], popt_dict['sy'] = popt_dict['sy'], popt_dict['sx']
            jac[:, [2, 3]] = jac[:, [3, 2]]
        elif 135 <= angle_diff < 225:
            angle = angle_offset + angle_diff - 180
        elif 225 <= angle_diff < 315:
            angle = angle_offset + angle_diff - 270
            popt_dict['sx'], popt_dict['sy'] = popt_dict['sy'], popt_dict['sx']
            jac[:, [2, 3]] = jac[:, [3, 2]]
        elif 315 <= angle_diff < 360:
            angle = angle_offset + angle_diff - 360
        popt_dict['angle'] = angle

    n_data_points = img_downsampled.shape[0]*img_downsampled.shape[1]
    n_fit_parameters = len(popt_dict)
    dof = n_data_points - n_fit_parameters
    sigma_squared = 2 * cost / dof
    try:
        cov = sigma_squared * np.linalg.inv(np.matmul(jac.T, jac))
    except np.linalg.LinAlgError as e:
        print(e)
        cov = 0 * jac

    fit_struct = create_fit_struct(img, popt_dict, cov, conf_level, dof, lightweight=lightweight)
    if show_plot or (save_name is not None):
        if not lightweight:
            make_visualization_figure(fit_struct, show_plot, save_name)
        else:
            print('Cannot visualize data with lightweight fit_struct')

    return fit_struct
