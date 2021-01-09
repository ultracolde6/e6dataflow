import numpy as np
from datamodel import DataTool
from utils import to_list, shot_to_loop_and_point


class Processor(DataTool):
    def __init__(self, *, processor_name, datamodel):
        super(Processor, self).__init__(datatool_name=processor_name)
        self.processor_name = processor_name
        self.datamodel = datamodel

    def process(self, shot_num):
        raise NotImplementedError


class CountsProcessor(Processor):
    def __init__(self, *, processor_name, datamodel, frame_datafield_name, result_datafield_name, roi_slice_list):
        super(CountsProcessor, self).__init__(processor_name=processor_name, datamodel=datamodel)
        self.frame_datafield_name = frame_datafield_name
        self.result_datafield_name = result_datafield_name
        self.roi_slice_list = to_list(roi_slice_list)
        self.datamodel = datamodel

        results_datafield =

    def process(self, shot_num):
        frame = self.datamodel.get_shot_data(self.frame_datafield_name, shot_num)
        if len(self.roi_slice_list) > 1:
            if len(self.roi_slice_list) != self.datamodel.num_points:
                raise ValueError(f'Length of roi_slice ({len(self.roi_slice_list):d}) is not equal to datamodel'
                                 f'num_points ({self.datamodel.num_points})')
            loop_num, point_num = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
            roi_slice = self.roi_slice_list[point_num]
            roi_frame = frame[roi_slice]
            counts = np.nansum(roi_frame)
        else:
            roi_slice = self.roi_slice_list[0]
            roi_frame = frame[roi_slice]
            counts = np.nansum(roi_frame)
        self.datamodel.set_shot_data(self.result_datafield_name, shot_num, counts)
