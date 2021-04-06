import numpy as np
from .datatool import DataTool, ShotHandler
from .utils import shot_to_loop_and_point


class Processor(ShotHandler):
    def __init__(self, *, name, parent_names):
        super(Processor, self).__init__(name=name, datatool_type=DataTool.PROCESSOR, parent_names=parent_names)

    def process(self, shot_num, quiet=False):
        self.handle(shot_num, quiet=quiet)

    def _handle(self, shot_num):
        self._process(shot_num)

    def _process(self, shot_num):
        raise NotImplementedError


class CountsProcessor(Processor):
    def __init__(self, *, name, frame_datafield_name, output_datafield_name, roi, parent_names):
        super(CountsProcessor, self).__init__(name=name, parent_names=parent_names)
        self.frame_datafield_name = frame_datafield_name
        self.output_datafield_name = output_datafield_name
        self.roi = roi
        self.mode = self.determine_roi_mode()

    def determine_roi_mode(self):
        if len(self.roi) == 2 and isinstance(self.roi[0], slice):
            mode = 'single_roi'
        elif isinstance(self.roi, list) or isinstance(self.roi, tuple):
            mode = 'roi_list'
        else:
            raise ValueError('roi_slice must be a single roi tuple or a list or tuple or roi tuples.')
        return mode

    def _process(self, shot_num):
        roi = None
        if self.mode == 'single_roi':
            roi = self.roi
        elif self.mode == 'roi_list':
            loop, point = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
            roi = self.roi[point]
        frame = self.datamodel.get_data(self.frame_datafield_name, shot_num)
        roi_frame = frame[roi]
        counts = np.nansum(roi_frame)
        self.datamodel.set_data(self.output_datafield_name, shot_num, counts)


class ThresholdProcessor(Processor):
    def __init__(self, *, name, input_datafield_name, output_datafield_name, threshold_value, parent_names):
        super(ThresholdProcessor, self).__init__(name=name, parent_names=parent_names)
        self.input_datafield_name = input_datafield_name
        self.output_datafield_name = output_datafield_name
        self.threshold_value = threshold_value
        self.child_name_list = [output_datafield_name]
        self.parent_name_list = [input_datafield_name]

    def _process(self, shot_num):
        data_value = self.datamodel.get_data(self.input_datafield_name, shot_num)
        verified = data_value > self.threshold_value
        self.datamodel.set_data(self.output_datafield_name, shot_num, verified)
