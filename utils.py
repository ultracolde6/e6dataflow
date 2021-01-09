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
