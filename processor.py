import numpy as np
from datamodel import DataTool
from utils import shot_to_loop_and_point


class Processor(DataTool):
    def __init__(self, *, name):
        super(Processor, self).__init__(name=name)

    def process(self, shot_num):
        raise NotImplementedError


class CountsProcessor(Processor):
    def __init__(self, *, name, frame_datafield_name, result_datafield_name, roi_slice):
        super(CountsProcessor, self).__init__(name=name)
        self.frame_datafield_name = frame_datafield_name
        self.result_datafield_name = result_datafield_name
        self.roi_slice = roi_slice

    def process(self, shot_num):
        frame = self.datamodel.get_shot_data(self.frame_datafield_name, shot_num)
        roi_frame = frame[self.roi_slice]
        counts = np.nansum(roi_frame)
        self.datamodel.set_shot_data(self.result_datafield_name, shot_num, counts)


class MultiCountsProcessor(Processor):
    def __init__(self, *, name, frame_datafield_name, result_datafield_name_list, roi_slice_array):
        super(MultiCountsProcessor, self).__init__(name=name)
        self.frame_datafield_name = frame_datafield_name
        self.result_datafield_name_list = result_datafield_name_list
        self.roi_slice_array = roi_slice_array
        self.num_points = self.datamodel.num_points
        self.num_regions = len(self.result_datafield_name_list)
        if roi_slice_array.shape is not (self.num_points, self.num_regions):
            raise ValueError(f'Shape of roi_slice_array much match number of points and number of output datafields.'
                             f' Shape must be ({self.num_points}, {self.num_regions})')

    def process(self, shot_num):
        frame = self.datamodel.get_shot_data(self.frame_datafield_name, shot_num)
        loop, point = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
        for roi_num, result_datafield_name in enumerate(self.result_datafield_name_list):
            roi_slice = self.roi_slice_array[point, roi_num]
            roi_frame = frame[roi_slice]
            counts = np.nansum(roi_frame)
            self.datamodel.set_shot_data(result_datafield_name, shot_num, counts)
