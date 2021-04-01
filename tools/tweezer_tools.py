import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
import h5py

from e6dataflow.tools.smart_gaussian2d_fit import fit_gaussian2d
from e6dataflow.utils import make_centered_roi, get_shot_list_from_point


def interpolate_tweezer_positions(first_tweezer_vert, first_tweezer_horiz,
                                  last_tweezer_vert, last_tweezer_horiz,
                                  num_tweezers):
    tweezer_vert_spacing = (last_tweezer_vert - first_tweezer_vert) / (num_tweezers - 1)
    tweezer_horiz_spacing = (last_tweezer_horiz - first_tweezer_horiz) / (num_tweezers - 1)
    vert_center_list = []
    horiz_center_list = []
    for tweezer_num in range(num_tweezers):
        vert_center = first_tweezer_vert + tweezer_num * tweezer_vert_spacing
        horiz_center = first_tweezer_horiz + tweezer_num * tweezer_horiz_spacing
        vert_center_list.append(vert_center)
        horiz_center_list.append(horiz_center)

    return vert_center_list, horiz_center_list


def fit_for_roi(img, vert_center, horiz_center, vert_span, horiz_span, lock_span=True, span_scale=3.0):

    horiz_halfspan = np.ceil(horiz_span / 2)
    lower_horiz = int(horiz_center - horiz_halfspan)
    upper_horiz = int(horiz_center + horiz_halfspan)

    vert_halfspan = np.ceil(vert_span / 2)
    lower_vert = int(vert_center - vert_halfspan)
    upper_vert = int(vert_center + vert_halfspan)

    fit_img = img[lower_vert:upper_vert, lower_horiz:upper_horiz]
    fit_dict = fit_gaussian2d(fit_img, show_plot=False, lightweight=True)

    horiz_span_result = int(horiz_span)
    vert_span_result = int(vert_span)
    success = fit_dict['success']
    if success:
        horiz0_result = fit_dict['x0']['val'] + lower_horiz
        vert0_result = fit_dict['y0']['val'] + lower_vert
        if not lock_span:
            horiz_span_result = int(span_scale * fit_dict['sx']['val'])
            vert_span_result = int(span_scale * fit_dict['sy']['val'])
    else:
        horiz0_result = horiz_center
        vert0_result = vert_center

    vert_slice, horiz_slice = make_centered_roi(vert0_result, horiz0_result, vert_span_result, horiz_span_result)
    return vert_slice, horiz_slice, success


def generate_pzt_point_frame_dict(num_pzt, num_point, num_frame, mode='single',
                                  num_inner_point_loop=None, num_outer_point_loop=None):
    pzt_point_frame_dict = dict()

    if mode == 'single':
        if num_pzt != 1:
            raise ValueError('num_pzt must equal 1 for mode=\'single\'.')
        point_frame_list = []
        for point_num in range(num_point):
            for frame_num in range(num_frame):
                point_frame_tuple = (point_num, frame_num)
                point_frame_list.append(point_frame_tuple)
        pzt_point_frame_dict[0] = point_frame_list

    if mode == 'frames':
        if num_pzt != num_frame:
            raise ValueError('num_pzt must equal num_frame for mode=\'frames\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            frame_num = pzt_num
            for point_num in range(num_point):
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
                for frame_num in range(num_frame):
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
                for frame_num in range(num_frame):
                    point_frame_tuple = (point_num, frame_num)
                    point_frame_list.append(point_frame_tuple)
            pzt_point_frame_dict[pzt_num] = point_frame_list

    if mode == 'points':
        if num_pzt != num_point:
            raise ValueError('num_pzt must equal num_point for mode=\'points\'.')
        for pzt_num in range(num_pzt):
            point_frame_list = []
            point_num = pzt_num
            for frame_num in range(num_frame):
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
    shot_list, num_loops = get_shot_list_from_point(point=point_num, num_points=num_points, num_shots=num_shots)
    first_shot_h5_path = Path(data_dir, f'{data_prefix}_00000.h5')
    avg_frame = np.zeros_like(get_frame_from_h5(first_shot_h5_path, frame_num=frame_num))
    for loop_num, shot_num in enumerate(shot_list):
        if shot_num <= max_shot_num:
            h5_path = Path(data_dir, f'{data_prefix}_{shot_num:05d}.h5')
            avg_frame += get_frame_from_h5(h5_path, frame_num=frame_num) / num_loops
            print(f'point: {point_num}, frame: {frame_num}, loop: {loop_num}, shot: {shot_num}')
    return avg_frame


def get_roi_dict(data_dir, data_prefix, num_points, pzt_point_frame_dict,
                 vert_center_list, horiz_center_list,
                 vert_span, horiz_span, lock_span=True, span_scale=3.0, max_shot_num=None):
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
                                                           vert_span=vert_span, horiz_span=horiz_span,
                                                           lock_span=lock_span,
                                                           span_scale=span_scale)
            roi_tuple_list.append((vert_slice, horiz_slice))
            roi_tuple_string = f'({vert_slice.start}, {vert_slice.stop}) x ({horiz_slice.start}, {horiz_slice.stop})'
            if not success:
                roi_tuple_string += ' FAIL'
            roi_tuple_string_list.append(roi_tuple_string)
            plot_vert_span = vert_slice.stop - vert_slice.start
            plot_horiz_span = horiz_slice.stop - horiz_slice.start
            rect = patches.Rectangle((horiz_slice.start, vert_slice.start),
                                     plot_horiz_span, plot_vert_span,
                                     linewidth=1, edgecolor='w', facecolor='none')
            ax.add_patch(rect)
        roi_string = ('ROI List\n (vert_start, vert_stop) x (horiz_start, horiz_stop)\n'
                      + '\n'.join([roi_tuple_string for roi_tuple_string in roi_tuple_string_list]))
        ax.text(x=2, y=0.5, s=roi_string, transform=ax.transAxes, verticalalignment='center')
        output_pzt_roi_dict[pzt_key] = roi_tuple_list

    return output_pzt_roi_dict
