import numpy as np
from datamodel import DataTool
from utils import shot_to_loop_and_point


class Verifier(DataTool):
    def __init__(self, *, verifier_name, datamodel):
        super(Verifier, self).__init__(datatool_name=verifier_name)
        self.verifier_name = verifier_name
        self.datamodel = datamodel
        self.verifier_list = np.array([])

    def condition_list(self, shot_num):
        current_length = len(self.verifier_list)
        new_minimum_length = shot_num + 1
        if current_length < new_minimum_length:
            new_list = np.full(new_minimum_length, True)
            new_list[0:current_length] = self.verifier_list
            self.verifier_list = new_list

    def verify(self, shot_num, shot_datafield_name):
        self.condition_list(shot_num)
        self._verify(shot_num, shot_datafield_name)

    def _verify(self, shot_num, shot_datafield_name):
        raise NotImplementedError


class ThresholdVerifier(Verifier):
    def __init__(self, *, verifier_name, datamodel, threshold_value):
        super(ThresholdVerifier, self).__init__(verifier_name=verifier_name, datamodel=datamodel)
        self.threshold_value = threshold_value

    def _verify(self, shot_num, shot_datafield_name):
        data = self.datamodel.get_shot_data(shot_datafield_name, shot_num)
        if not isinstance(self.threshold_value, list):
            verified = data >= self.threshold_value
        else:
            loop, point = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
            threshold_single = self.threshold_value[point]
            verified = data >= threshold_single
        self.verifier_list[shot_num] = verified