import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
import h5py

from e6dataflow.tools.smart_gaussian2d_fit import fit_gaussian2d
from e6dataflow.utils import make_centered_roi, get_shot_list_from_point


def interpolate_tweezer_positions(first_tweezer_vert, first_tweezer_horiz,
                                  last_tweezer_vert, last_tweezer_horiz,
                                  num_tweezer):
    tweezer_vert_spacing = (last_tweezer_vert - first_tweezer_vert) / (num_tweezer - 1)
    tweezer_horiz_spacing = (last_tweezer_horiz - first_tweezer_horiz) / (num_tweezer - 1)
    vert_center_list = []
    horiz_center_list = []
    for tweezer_num in range(num_tweezer):
        vert_center = first_tweezer_vert + tweezer_num * tweezer_vert_spacing
        horiz_center = first_tweezer_horiz + tweezer_num * tweezer_horiz_spacing
        vert_center_list.append(vert_center)
        horiz_center_list.append(horiz_center)

    return vert_center_list, horiz_center_list


def fit_for_roi(img, vert_center_guess, horiz_center_guess, vert_search_span, horiz_search_span,
                lock_span=True, span_output_factor=3.0):

    horiz_halfspan = np.ceil(horiz_search_span / 2)
    lower_horiz = int(horiz_center_guess - horiz_halfspan)
    upper_horiz = int(horiz_center_guess + horiz_halfspan)

    vert_halfspan = np.ceil(vert_search_span / 2)
    lower_vert = int(vert_center_guess - vert_halfspan)
    upper_vert = int(vert_center_guess + vert_halfspan)

    fit_img = img[lower_vert:upper_vert, lower_horiz:upper_horiz]
    fit_dict = fit_gaussian2d(fit_img, show_plot=False, lightweight=True)

    horiz_center_fit = fit_dict['x0']['val'] + lower_horiz
    vert_center_fit = fit_dict['y0']['val'] + lower_vert
    horiz_sigma_fit = fit_dict['sx']['val']
    vert_sigma_fit = fit_dict['sy']['val']

    success = fit_dict['success']
    if not lower_horiz < horiz_center_fit < upper_horiz:
        success = False
    if not lower_vert < vert_center_fit < upper_vert:
        success = False
    if not horiz_search_span / 10 < horiz_sigma_fit < horiz_search_span:
        success = False
    if not vert_search_span / 10 < vert_sigma_fit < vert_search_span:
        success = False

    if success:
        horiz_center_output = horiz_center_fit
        vert_center_output = vert_center_fit
        if not lock_span:
            horiz_span_output = int(span_output_factor * horiz_sigma_fit)
            vert_span_output = int(span_output_factor * vert_sigma_fit)
        else:
            horiz_span_output = horiz_search_span
            vert_span_output = vert_search_span
    else:
        horiz_center_output = horiz_center_guess
        vert_center_output = vert_center_guess
        horiz_span_output = horiz_search_span
        vert_span_output = vert_search_span

    vert_slice, horiz_slice = make_centered_roi(vert_center_output, horiz_center_output,
                                                vert_span_output, horiz_span_output)
    return vert_slice, horiz_slice, success


def generate_pzt_point_frame_dict(num_pzt, num_points, frame_list, mode='single',
                                  num_inner_point_loop=None, num_outer_point_loop=None):
    pzt_point_frame_dict = dict()
    num_frames = len(frame_list)

    if mode == 'single':
        if num_pzt != 1:
            raise ValueError('num_pzt must equal 1 for mode=\'single\'.')
        point_frame_list = []
        for point_num in range(num_points):
            for frame_num in frame_list:
                point_frame_tuple = (point_num, frame_num)
                point_frame_list.append(point_frame_tuple)
        pzt_point_frame_dict[0] = point_frame_list

    if mode == 'frames':
        if num_pzt != num_frames:
            raise ValueError('num_pzt must equal num_frames for mode=\'frames\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            frame_num = pzt_num
            for point_num in range(num_points):
                point_frame_tuple = (point_num, frame_num)
                point_frame_list.append(point_frame_tuple)
            pzt_point_frame_dict[pzt_num] = point_frame_list

    if mode == 'point_outer_point_loop':
        if num_pzt != num_outer_point_loop:
            raise ValueError('num_pzt must equal points_outer_point_loop '
                             'for mode=\'points_outer_point_loop\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            outer_point_num = pzt_num
            for inner_point_num in range(num_inner_point_loop):
                point_num = outer_point_num * num_inner_point_loop + inner_point_num
                for frame_num in frame_list:
                    point_frame_tuple = (point_num, frame_num)
                    point_frame_list.append(point_frame_tuple)
            pzt_point_frame_dict[pzt_num] = point_frame_list

    if mode == 'points_inner_point_loop':
        if num_pzt != num_inner_point_loop:
            raise ValueError('num_pzt must equal points_inner_point_loop '
                             'for mode=\'points_inner_point_loop\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            inner_point_num = pzt_num
            for outer_point_num in range(num_outer_point_loop):
                point_num = outer_point_num * num_inner_point_loop + inner_point_num
                for frame_num in frame_list:
                    point_frame_tuple = (point_num, frame_num)
                    point_frame_list.append(point_frame_tuple)
            pzt_point_frame_dict[pzt_num] = point_frame_list

    if mode == 'points':
        if num_pzt != num_points:
            raise ValueError('num_pzt must equal num_points for mode=\'points\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            point_num = pzt_num
            for frame_num in frame_list:
                point_frame_tuple = (point_num, frame_num)
                point_frame_list.append(point_frame_tuple)
            pzt_point_frame_dict[pzt_num] = point_frame_list

    return pzt_point_frame_dict


def get_num_shots(data_dir):
    num_shots = len(list(data_dir.glob('*.h5')))
    return num_shots


def get_frame_from_h5(h5_path, frame_num):
    frame_key = f'frame-{frame_num:02d}'
    with h5py.File(h5_path, 'r') as h5_file:
        data = np.array(h5_file[frame_key][:].astype(float))
    return data


def get_avg_frame_over_loops(data_dir, data_prefix, point_num, frame_num, num_points, max_shot_num=None):
    num_shots = get_num_shots(data_dir)
    if max_shot_num is None:
        max_shot_num = num_shots
    shot_list, num_loops = get_shot_list_from_point(point=point_num, num_points=num_points, num_shots=max_shot_num)
    first_shot_h5_path = Path(data_dir, f'{data_prefix}_00000.h5')
    avg_frame = np.zeros_like(get_frame_from_h5(first_shot_h5_path, frame_num=frame_num))
    for loop_num, shot_num in enumerate(shot_list):
        h5_path = Path(data_dir, f'{data_prefix}_{shot_num:05d}.h5')
        avg_frame += get_frame_from_h5(h5_path, frame_num=frame_num) / num_loops
    return avg_frame


def get_roi_dict(data_dir, data_prefix, num_points, pzt_point_frame_dict,
                 vert_center_list, horiz_center_list,
                 vert_search_span, horiz_search_span, lock_span=True, span_output_factor=3.0, max_shot_num=None):
    first_shot_h5_path = Path(data_dir, f'{data_prefix}_00000.h5')
    blank_frame = np.zeros_like(get_frame_from_h5(first_shot_h5_path, frame_num=0))
    num_tweezer = len(vert_center_list)
    output_pzt_roi_dict = dict()
    for pzt_key, point_frame_tuple_list in pzt_point_frame_dict.items():
        num_elements = len(point_frame_tuple_list)
        tot_avg_frame = 0 * blank_frame
        roi_tuple_list = []
        roi_tuple_string_list = []
        for point_frame_tuple in point_frame_tuple_list:
            point_num = point_frame_tuple[0]
            frame_num = point_frame_tuple[1]
            loop_avg_frame = get_avg_frame_over_loops(data_dir, data_prefix, point_num, frame_num, num_points,
                                                      max_shot_num=max_shot_num)
            tot_avg_frame += loop_avg_frame / num_elements
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        ax.imshow(tot_avg_frame)
        for tweezer_num in range(num_tweezer):
            vert_center = vert_center_list[tweezer_num]
            horiz_center = horiz_center_list[tweezer_num]
            vert_slice, horiz_slice, success = fit_for_roi(tot_avg_frame,
                                                           vert_center, horiz_center,
                                                           vert_search_span=vert_search_span,
                                                           horiz_search_span=horiz_search_span,
                                                           lock_span=lock_span,
                                                           span_output_factor=span_output_factor)
            roi_tuple_list.append((vert_slice, horiz_slice))
            roi_tuple_string = f'({vert_slice.start}, {vert_slice.stop}) x ({horiz_slice.start}, {horiz_slice.stop})'
            if not success:
                roi_tuple_string += ' FAIL'
            roi_tuple_string_list.append(roi_tuple_string)
            plot_vert_span = vert_slice.stop - vert_slice.start
            plot_horiz_span = horiz_slice.stop - horiz_slice.start
            rect_guess = patches.Rectangle((horiz_center - horiz_search_span / 2, vert_center - vert_search_span / 2),
                                           horiz_search_span, vert_search_span,
                                           linewidth=1, edgecolor='r', facecolor='none')
            rect_fit = patches.Rectangle((horiz_slice.start, vert_slice.start),
                                         plot_horiz_span, plot_vert_span,
                                         linewidth=1, edgecolor='w', facecolor='none')
            ax.add_patch(rect_guess)
            ax.add_patch(rect_fit)
        roi_string = ('ROI List\n (vert_start, vert_stop) x (horiz_start, horiz_stop)\n'
                      + '\n'.join([roi_tuple_string for roi_tuple_string in roi_tuple_string_list]))
        ax.text(x=2, y=0.5, s=roi_string, transform=ax.transAxes, verticalalignment='center')
        output_pzt_roi_dict[pzt_key] = roi_tuple_list

    return output_pzt_roi_dict


def auto_roi(data_dir, data_prefix,
             num_tweezers, num_points, frame_list, num_pzt, mode,
             first_tweezer_vert, first_tweezer_horiz,
             last_tweezer_vert, last_tweezer_horiz,
             vert_search_span, horiz_search_span, lock_span):
    vert_center_list, horiz_center_list = interpolate_tweezer_positions(first_tweezer_vert,
                                                                        first_tweezer_horiz,
                                                                        last_tweezer_vert,
                                                                        last_tweezer_horiz,
                                                                        num_tweezers)

    pzt_point_frame_dict = generate_pzt_point_frame_dict(num_pzt=num_pzt, num_points=num_points,
                                                         frame_list=frame_list, mode=mode)

    pzt_roi_dict = get_roi_dict(data_dir=data_dir, data_prefix=data_prefix,
                                num_points=num_points,
                                pzt_point_frame_dict=pzt_point_frame_dict,
                                vert_center_list=vert_center_list,
                                horiz_center_list=horiz_center_list,
                                vert_search_span=vert_search_span,
                                horiz_search_span=horiz_search_span,
                                lock_span=lock_span)
    return pzt_roi_dict, pzt_point_frame_dict


def get_roi_list_by_point(pzt_roi_dict, pzt_point_frame_dict, num_points, frame_num, tweezer_num):
    roi_list = []
    for point_num in range(num_points):
        for pzt_num, point_frame_tuple_list in pzt_point_frame_dict.items():
            for point_frame_tuple in point_frame_tuple_list:
                point_num_to_check = point_frame_tuple[0]
                frame_num_to_check = point_frame_tuple[1]
                if point_num_to_check == point_num and frame_num_to_check == frame_num:
                    new_roi = pzt_roi_dict[pzt_num][tweezer_num]
                    roi_list.append(new_roi)
    return roi_list
