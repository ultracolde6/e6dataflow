from datamodel import DataTool
from utils import shot_to_loop_and_point, get_shot_list_from_point, list_intersection


class Aggregator(DataTool):
    def __init__(self, *, name, verifier_datafield_names):
        super(Aggregator, self).__init__(name=name, datatool_type=DataTool.AGGREGATOR)
        self.verifier_datafield_names = verifier_datafield_names
        self.aggregated_shots = []
        self.num_aggregated_shots = 0

    def reset(self):
        super(Aggregator, self).reset()
        self.aggregated_shots = []
        self.num_aggregated_shots = 0

    def aggregate(self, shot_num):
        if shot_num not in self.aggregated_shots:
            verified = True
            for verifier_datafield_name in self.verifier_datafield_names:
                if not self.datamodel.get_data(verifier_datafield_name, shot_num):
                    verified = False
            if verified:
                print(f'aggregating shot {shot_num:05d} with "{self.name}" aggregator')
                self._aggregate(shot_num)
                self.aggregated_shots.append(shot_num)
        else:
            print(f'skipping shot {shot_num:05d} with "{self.name}" aggregator')

    def _aggregate(self, shot_num):
        raise NotImplementedError

    def package_rebuild_dict(self):
        super(Aggregator, self).package_rebuild_dict()
        self.object_data_dict['aggregated_shots'] = self.aggregated_shots

    def rebuild_object_data(self, object_data_dict):
        super(Aggregator, self).rebuild_object_data(object_data_dict)
        self.aggregated_shots = object_data_dict['aggregated_shots']


class AverageStdAggregator(Aggregator):
    def __init__(self, *, name, verifier_datafield_names, input_datafield_name, output_datafield_name):
        super(AverageStdAggregator, self).__init__(name=name, verifier_datafield_names=verifier_datafield_names)
        self.input_datafield_name = input_datafield_name
        self.output_datafield_name = output_datafield_name

    def link_within_datamodel(self):
        super(AverageStdAggregator, self).link_within_datamodel()
        self.add_child(self.output_datafield_name)
        self.add_parent(self.input_datafield_name)

    def _aggregate(self, shot_num):
        loop_num, point_num = shot_to_loop_and_point(shot_num, self.datamodel.num_points)
        point_shot_list, num_loops = get_shot_list_from_point(point_num, self.datamodel.num_points, shot_num)
        point_aggregated_shots = list_intersection(point_shot_list, self.aggregated_shots)
        old_n = len(point_aggregated_shots)
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
