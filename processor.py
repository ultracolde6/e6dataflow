import numpy as np
from datamodel import DataTool
from utils import shot_to_loop_and_point


class Processor(DataTool):
    def __init__(self, *, name):
        super(Processor, self).__init__(name=name, datatool_type=DataTool.PROCESSOR)
        self.processed_shots = []

    def reset(self):
        super(Processor, self).reset()
        self.processed_shots = []

    def process(self, shot_num):
        if shot_num not in self.processed_shots:
            print(f'processing shot {shot_num:05d} with "{self.name}" processor')
            self._process(shot_num)
            self.processed_shots.append(shot_num)
        else:
            print(f'skipping shot {shot_num:05d} with "{self.name}" processor')

    def _process(self, shot_num):
        raise NotImplementedError

    def package_rebuild_dict(self):
        super(Processor, self).package_rebuild_dict()
        self.object_data_dict['processed_shots'] = self.processed_shots

    def rebuild_object_data(self, object_data_dict):
        super(Processor, self).rebuild_object_data(object_data_dict)
        self.processed_shots = object_data_dict['processed_shots']


class CountsProcessor(Processor):
    def __init__(self, *, name, frame_datafield_name, output_datafield_name, roi_slice):
        super(CountsProcessor, self).__init__(name=name)
        self.frame_datafield_name = frame_datafield_name
        self.result_datafield_name = output_datafield_name
        self.roi_slice = roi_slice

    def set_datamodel(self, datamodel):
        super(CountsProcessor, self).set_datamodel(datamodel)
        self.add_child(self.result_datafield_name)
        self.add_parent(self.frame_datafield_name)

    def _process(self, shot_num):
        frame = self.datamodel.get_data(self.frame_datafield_name, shot_num)
        roi_frame = frame[self.roi_slice]
        counts = np.nansum(roi_frame)
        self.datamodel.set_data(self.result_datafield_name, shot_num, counts)


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

    def set_datamodel(self, datamodel):
        super(MultiCountsProcessor, self).set_datamodel(datamodel)
        for result_datafield_name in self.result_datafield_name_list:
            self.add_child(result_datafield_name)
        self.add_parent(self.frame_datafield_name)

    def _process(self, shot_num):
        frame = self.datamodel.get_data(self.frame_datafield_name, shot_num)
        loop, point = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
        for roi_num, result_datafield_name in enumerate(self.result_datafield_name_list):
            roi_slice = self.roi_slice_array[point, roi_num]
            roi_frame = frame[roi_slice]
            counts = np.nansum(roi_frame)
            self.datamodel.set_data(result_datafield_name, shot_num, counts)


class ThresholdProcessor(Processor):
    def __init__(self, *, name, input_datafield_name, output_datafield_name, threshold_value):
        super(ThresholdProcessor, self).__init__(name=name)
        self.input_datafield_name = input_datafield_name
        self.output_datafield_name = output_datafield_name
        self.threshold_value = threshold_value

        self.add_child(self.output_datafield_name)
        self.add_parent(self.input_datafield_name)

    def _process(self, shot_num):
        data_value = self.datamodel.get_data(self.input_datafield_name, shot_num)
        verified = data_value > self.threshold_value
        self.datamodel.set_data(self.output_datafield_name, shot_num, verified)
