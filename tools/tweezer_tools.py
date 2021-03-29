import numpy as np
from e6dataflow.tools.smart_gaussian2d_fit import fit_gaussian2d
from e6dataflow.utils import make_centered_roi


def estimate_tweezer_centers(first_tweezer_vert, first_tweezer_horiz,
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


def fit_for_roi(img, vert_center, horiz_center, vert_span, horiz_span, lock_span=True):

    horiz_halfspan = np.ceil(horiz_span / 2)
    lower_horiz = int(horiz_center - horiz_halfspan)
    upper_horiz = int(horiz_center + horiz_halfspan)
    print(f'lower_x = {lower_horiz}, upper_x = {upper_horiz}')

    vert_halfspan = np.ceil(vert_span / 2)
    lower_vert = int(vert_center - vert_halfspan)
    upper_vert = int(vert_center + vert_halfspan)
    print(f'lower_y = {lower_vert}, upper_y = {upper_vert}')

    fit_img = img[lower_vert:upper_vert, lower_horiz:upper_horiz]
    fit_dict = fit_gaussian2d(fit_img)

    horiz0_result = fit_dict['x0']['val'] + lower_horiz
    vert0_result = fit_dict['y0']['val'] + lower_vert
    if not lock_span:
        horiz_span_result = int(3 * fit_dict['sx']['val'])
        vert_span_result = int(3 * fit_dict['sy']['val'])
    else:
        horiz_span_result = int(horiz_span)
        vert_span_result = int(vert_span)

    vert_slice, horiz_slice = make_centered_roi(vert0_result, horiz0_result, vert_span_result, horiz_span_result)
    return vert_slice, horiz_slice


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


def calc_roi_from_mean_image(datamodel, pzt_point_frame_dict, vert_center_list, horiz_center_list,
                             vert_span, horiz_span, lock_span=True):
    blank_img = 0 * datamodel.get_data('frame-00_mean', data_index=0)
    num_tweezer = len(vert_center_list)
    output_pzt_roi_dict = dict()
    for pzt_key, point_frame_tuple_list in pzt_point_frame_dict.items():
        num_elements = len(point_frame_tuple_list)
        mean_img = 0 * blank_img
        roi_tuple_list = []
        for point_frame_tuple in point_frame_tuple_list:
            point_num = point_frame_tuple[0]
            frame_num = point_frame_tuple[1]
            frame_key = f'frame-{frame_num:02d}_mean'
            img = datamodel.get_data(frame_key, point_num)
            mean_img += img / num_elements
        for tweezer_num in range(num_tweezer):
            vert_center = vert_center_list[tweezer_num]
            horiz_center = horiz_center_list[tweezer_num]
            vert_slice, horiz_slice = fit_for_roi(mean_img,
                                                  vert_center[tweezer_num], horiz_center[tweezer_num],
                                                  vert_span=vert_span, horiz_span=horiz_span,
                                                  lock_span=lock_span)
            roi_tuple_list.append((vert_slice, horiz_slice))
        output_pzt_roi_dict[pzt_key] = roi_tuple_list
    return output_pzt_roi_dict
