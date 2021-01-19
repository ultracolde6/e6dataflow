from .datatool import DataTool, ShotHandler
from .utils import shot_to_loop_and_point, get_shot_list_from_point, list_intersection


class Aggregator(ShotHandler):
    def __init__(self, *, name, verifier_datafield_names):
        super(Aggregator, self).__init__(name=name, datatool_type=DataTool.AGGREGATOR)
        self.verifier_datafield_names = verifier_datafield_names

    def aggregate(self, shot_num):
        self.handle(shot_num)

    def _handle(self, shot_num):
        if self.verify_shot(shot_num):
            self._aggregate(shot_num)

    def _aggregate(self, shot_num):
        raise NotImplementedError

    def verify_shot(self, shot_num):
        verified = True
        for verifier_datafield_name in self.verifier_datafield_names:
            if not self.datamodel.get_data(verifier_datafield_name, shot_num):
                verified = False
        return verified


class AverageStdAggregator(Aggregator):
    def __init__(self, *, name, verifier_datafield_names, input_datafield_name, output_datafield_name):
        super(AverageStdAggregator, self).__init__(name=name, verifier_datafield_names=verifier_datafield_names)
        self.input_datafield_name = input_datafield_name
        self.output_datafield_name = output_datafield_name
        self.num_aggregated_shots_list = None

    def reset(self):
        super(AverageStdAggregator, self).reset()
        self.num_aggregated_shots_list = [0] * self.datamodel.num_points

    def link_within_datamodel(self):
        super(AverageStdAggregator, self).link_within_datamodel()
        self.add_child(self.output_datafield_name)
        self.add_parent(self.input_datafield_name)
        if self.num_aggregated_shots_list is None:
            self.num_aggregated_shots_list = [0] * self.datamodel.num_points

    def _aggregate(self, shot_num):
        loop_num, point_num = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
        # point_shot_list, num_loops = get_shot_list_from_point(point_num, self.datamodel.num_points, shot_num)
        # point_aggregated_shots = list_intersection(point_shot_list, self.aggregated_shots)
        old_n = self.num_aggregated_shots_list[point_num]
        new_n = old_n + 1
        self.num_aggregated_shots_list[point_num] = new_n
        new_data = self.datamodel.get_data(self.input_datafield_name, shot_num)
        try:
            old_result_dict = self.datamodel.get_data(self.output_datafield_name, point_num)
            old_mean = old_result_dict['mean']
            old_std = old_result_dict['std']
            new_mean = self.calculate_new_mean(old_mean, old_n, new_data)
            new_std = self.calculate_new_std(old_mean, old_std, old_n, new_data)
        except KeyError:
            new_mean = new_data
            new_std = 0
        result_dict = {'mean': new_mean, 'std': new_std}
        self.datamodel.set_data(self.output_datafield_name, point_num, result_dict)

    @staticmethod
    def calculate_new_mean(old_mean, old_n, new_data):
        new_n = old_n + 1
        new_mean = old_mean + (new_data - old_mean) / new_n
        return new_mean

    @classmethod
    def calculate_new_std(cls, old_mean, old_std, old_n, new_data):
        new_n = old_n + 1
        new_mean = cls.calculate_new_mean(old_mean, old_n, new_data)
        old_variance = old_std ** 2
        old_sos = (old_n - 1) * old_variance
        new_sos = old_sos + (new_data - old_mean) * (new_data - new_mean)
        new_variance = new_sos / (new_n - 1)
        new_std = new_variance ** (1 / 2)
        return new_std

    def package_rebuild_dict(self):
        super(AverageStdAggregator, self).package_rebuild_dict()
        self.object_data_dict['num_aggregated_shots_list'] = self.num_aggregated_shots_list

    def rebuild_object_data(self, object_data_dict):
        super(AverageStdAggregator, self).rebuild_object_data(object_data_dict)
        self.num_aggregated_shots_list = object_data_dict['num_aggregated_shots_list']
