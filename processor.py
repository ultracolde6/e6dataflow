import numpy as np
from .datatool import DataTool, ShotHandler
from .utils import shot_to_loop_and_point


class Processor(ShotHandler):
    def __init__(self, *, name):
        super(Processor, self).__init__(name=name, datatool_type=DataTool.PROCESSOR)

    def process(self, shot_num):
        self.handle(shot_num)

    def _handle(self, shot_num):
        self._process(shot_num)

    def _process(self, shot_num):
        raise NotImplementedError


class CountsProcessor(Processor):
    def __init__(self, *, name, frame_datafield_name, output_datafield_name, roi_slice):
        super(CountsProcessor, self).__init__(name=name)
        self.frame_datafield_name = frame_datafield_name
        self.result_datafield_name = output_datafield_name
        self.roi_slice = roi_slice

    def link_within_datamodel(self):
        super(CountsProcessor, self).link_within_datamodel()
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

    def link_within_datamodel(self):
        super(MultiCountsProcessor, self).link_within_datamodel()
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

    def link_within_datamodel(self):
        super(ThresholdProcessor, self).link_within_datamodel()
        self.add_child(self.output_datafield_name)
        self.add_parent(self.input_datafield_name)

    def _process(self, shot_num):
        data_value = self.datamodel.get_data(self.input_datafield_name, shot_num)
        verified = data_value > self.threshold_value
        self.datamodel.set_data(self.output_datafield_name, shot_num, verified)
