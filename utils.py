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
    vert_lower = vert_center - vert_span / 2
    vert_upper = vert_center + vert_span / 2
    horiz_lower = horiz_center - horiz_span / 2
    horiz_upper = horiz_center + horiz_span / 2
    vert_slice = slice(vert_lower, vert_upper, 1)
    horiz_slice = slice(horiz_lower, horiz_upper, 1)
    return vert_slice, horiz_slice
