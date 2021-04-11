import numpy as np


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
    shot_list = np.arange(point, num_shots, num_points)
    start_mask = start_shot <= shot_list
    shot_list = shot_list[start_mask]
    if stop_shot is not None:
        stop_mask = shot_list <= stop_shot
        shot_list = shot_list[stop_mask]
    num_loops = len(shot_list)
    return shot_list, num_loops


def get_shot_labels(shot_num, num_points):
    loop_num, point_num = shot_to_loop_and_point(shot_num, num_points=num_points)
    loop_key = f'loop_{loop_num:05d}'
    point_key = f'point_{point_num:02d}'
    shot_key = f'shot_{shot_num:05d}'
    return shot_key, loop_key, point_key


def outer_inner_to_point(outer_num, inner_num, num_outer, num_inner):
    if inner_num >= num_inner:
        raise ValueError('inner_num cannot exceed num_inner')
    if outer_num >= num_outer:
        raise ValueError('outer_num cannot exceed num_outer')
    return outer_num * num_inner + inner_num


def point_to_outer_and_inner(point, num_inner):
    outer_num = point // num_inner
    inner_num = point % num_inner
    return outer_num, inner_num


def list_intersection(list_1, list_2):
    result = list(set(list_1) and set(list_2))
    return result


def qprint(string, quiet):
    if not quiet:
        print(string)


def make_centered_roi(vert_center, horiz_center, vert_span, horiz_span, max_vert=None, max_horiz=None):
    vert_lower = max(int(vert_center - vert_span / 2), 0)
    vert_upper = int(vert_center + vert_span / 2)
    horiz_lower = max(int(horiz_center - horiz_span / 2), 0)
    horiz_upper = int(horiz_center + horiz_span / 2)
    if max_vert is not None:
        vert_upper = min(vert_upper, max_vert)
    if max_horiz is not None:
        horiz_upper = min(horiz_upper, max_horiz)
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


def scale_range(range_min, range_max, scale_factor):
    range_center = (range_max + range_min) / 2
    range_span = range_max - range_min
    new_range_span = range_span * scale_factor
    new_range_min = range_center - new_range_span / 2
    new_range_max = range_center + new_range_span / 2
    return new_range_min, new_range_max
