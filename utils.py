import numpy as np
import matplotlib.pyplot as plt
import e6py.smart_gaussian2d_fit as e6fit

def get_data_min_max(data):
    if isinstance(data, list):
        data_min = min(data)
        data_max = max(data)
    elif isinstance(data, np.ndarray):
        data_min = data.min()
        data_max = data.max()
    else:
        print('Unable to extract data minimum and maximum values.')
        raise ValueError
    return data_min, data_max


def to_list(var):
    """
    Helper function to convert singleton input parameters into list-expecting parameters into singleton lists.
    """
    if isinstance(var, (list, tuple)):
        return var
    else:
        return [var]


def shot_to_loop_and_point(shot, num_points=1,
                           shot_index_convention=0,
                           loop_index_convention=0,
                           point_index_convention=0):
    """
    Convert shot number to loop and point using the number of points. Default assumption is indexing for
    shot, loop, and point all starts from zero with options for other conventions.
    """
    shot_ind = shot - shot_index_convention
    loop_ind = shot_ind // num_points
    point_ind = shot_ind % num_points
    loop = loop_ind + loop_index_convention
    point = point_ind + point_index_convention
    return loop, point


def get_shot_list_from_point(point, num_points, num_shots, start_shot=0, stop_shot=None):
    # TODO: Implement different conventions for shot and point start indices
    shots = np.arange(point, num_shots, num_points)
    start_mask = start_shot <= shots
    shots = shots[start_mask]
    if stop_shot is not None:
        stop_mask = shots <= stop_shot
        shots = shots[stop_mask]
    num_loops = len(shots)
    return shots, num_loops


def get_shot_labels(shot_num, num_points):
    loop_num, point_num = shot_to_loop_and_point(shot_num, num_points=num_points)
    loop_key = f'loop_{loop_num:05d}'
    point_key = f'point_{point_num:02d}'
    shot_key = f'shot_{shot_num:05d}'
    return shot_key, loop_key, point_key


def list_intersection(list_1, list_2):
    result = list(set(list_1) and set(list_2))
    return result


def qprint(string, quiet):
    if not quiet:
        print(string)


def make_centered_roi(vert_center, horiz_center, vert_span, horiz_span):
    vert_lower = int(vert_center - vert_span / 2)
    vert_upper = int(vert_center + vert_span / 2)
    horiz_lower = int(horiz_center - horiz_span / 2)
    horiz_upper = int(horiz_center + horiz_span / 2)
    vert_slice = slice(vert_lower, vert_upper, 1)
    horiz_slice = slice(horiz_lower, horiz_upper, 1)
    return vert_slice, horiz_slice


def dict_compare(dict_1, dict_2):
    for key in dict_1.keys():
        if key not in dict_2:
            return False
        else:
            element_1 = dict_1[key]
            element_2 = dict_2[key]
            if isinstance(element_1, dict) and isinstance(element_2, dict):
                if not dict_compare(element_1, element_2):
                    return False
            elif isinstance(element_1, np.ndarray) and isinstance(element_2, np.ndarray):
                if not np.all(element_1 == element_2):
                    return False
            elif type(element_1) == type(element_2):
                if not element_1 == element_2:
                    return False
            else:
                return False
    return True


def ROI_fit(fit_frame_array,roi_guess_array,quiet=True,iterations=1,roi_final_shape=None):
    num_pts = roi_guess_array.shape[0]
    num_twz = roi_guess_array.shape[1]
    result = {}
    if roi_final_shape == None:
        roi_final_shape = (roi_guess_array[0,0][0].stop - roi_guess_array[0,0][0].start,
                           roi_guess_array[0,0][1].stop -roi_guess_array[0,0][1].start)

    for i in range(iterations):
        result[f'iteration-{i:01d}'] = {}
        if not quiet:
            print('iteration ',i)
        for pt in range(num_pts):
            frame = fit_frame_array[pt,:,:]
            res = {}
            for twz in range(num_twz):
                roi = roi_guess_array[pt,twz]
                if i == 0:
                    fit_struct = e6fit.fit_gaussian2d(frame[roi],
                                                      fix_angle=True,fix_lin_slope=True,show_plot=False)
                else:
                    fit_struct = e6fit.fit_gaussian2d(frame[roi],
                                                      fix_angle=True,fix_lin_slope=True,show_plot=False,
                                                      guess=[(roi[1].stop - roi[1].start)/2,
                                                             (roi[0].stop - roi[0].start)/2,
                                                             result[f'iteration-{(i-1):01d}'][f'point-{pt:02d}'][
                                                                 f'tweezer-{twz:02d}']['sx']['val'],
                                                             result[f'iteration-{(i-1):01d}'][f'point-{pt:02d}'][
                                                                 f'tweezer-{twz:02d}']['sy']['val'],
                                                             result[f'iteration-{(i-1):01d}'][f'point-{pt:02d}'][
                                                                 f'tweezer-{twz:02d}']['amp']['val'],
                                                             result[f'iteration-{(i-1):01d}'][f'point-{pt:02d}'][
                                                                 f'tweezer-{twz:02d}']['offset']['val']])
                for key in ['val','val_lb','val_ub']:
                    fit_struct['x0'][key]+=roi[1].start
                    fit_struct['y0'][key]+=roi[0].start
                res[f'tweezer-{twz:02d}']=fit_struct
                centered_roi = make_centered_roi(vert_center=fit_struct['y0']['val'], \
                                                 horiz_center=fit_struct['x0']['val'], \
                                                 vert_span=roi[0].stop - roi[0].start, \
                                                 horiz_span=roi[1].stop - roi[1].start)
                if i+1 == iterations:
                    centered_roi = make_centered_roi(vert_center=fit_struct['y0']['val'], \
                                                     horiz_center=fit_struct['x0']['val'], \
                                                     vert_span=roi_final_shape[0], \
                                                     horiz_span=roi_final_shape[1])
                roi_guess_array[pt,twz]=centered_roi
            result[f'iteration-{i:01d}'][f'point-{pt:02d}'] = res
        if not quiet:
            for twz in range(num_twz):
                fig = plt.figure()
                fig.suptitle(f'tweezer-{twz:02d}, iteration-{i:01d}')
                for pt in range(num_pts):
                    plt.subplot(10,8,1+2*pt)
                    plt.imshow(result[f'iteration-{i:01d}'][f'point-{pt:02d}'][f'tweezer-{twz:02d}']['data_img'])
                    plt.axis('off')
                    plt.subplot(10,8,2+2*pt)
                    plt.imshow(result[f'iteration-{i:01d}'][f'point-{pt:02d}'][f'tweezer-{twz:02d}']['model_img'])
                    plt.axis('off')
                plt.show()
    return result, roi_guess_array