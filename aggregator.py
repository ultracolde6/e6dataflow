from datamodel import DataTool


class Aggregator(DataTool):
    def __init__(self, *, name, verifier_datafield_names):
        super(Aggregator, self).__init__(name=name, datatool_type=DataTool.AGGREGATOR)
        self.verifier_datafield_names = verifier_datafield_names
        self.aggregated_shots = []
        self.num_aggregated_shots = 0

    def aggregate(self, shot_num):
        verified = True
        for verifier_datafield_name in self.verifier_datafield_names:
            if not self.datamodel.get_shot_data(verifier_datafield_name, shot_num):
                verified = False
        if verified:
            self._aggregate(shot_num)
            self.aggregated_shots.append(shot_num)

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

    def _aggregate(self, point_num):
        new_data = self.datamodel.get_shot_data(self.input_datafield_name, point_num)
        try:
            old_mean = self.datamodel.get_point_data(self.output_datafield_name)['mean']
            old_std = self.datamodel.get_point_data(self.output_datafield_name)['std']
            old_n = len(self.aggregated_shots)
            new_mean = self.calculate_new_mean(old_mean, old_n, new_data)
            new_std = self.calculate_new_std(old_mean, old_std, old_n, new_data)
        except KeyError:
            new_mean = new_data
            new_std = 0
        result_dict = {'mean': new_mean, 'std': new_std}
        self.datamodel.set_point_data(self.output_datafield_name, result_dict)

    @staticmethod
    def calculate_new_mean(old_mean, old_n, new_data):
        new_mean = (old_mean * old_n + new_data) / (old_n + 1)
        return new_mean

    @classmethod
    def calculate_new_std(cls, old_mean, old_std, old_n, new_data):
        new_n = old_n + 1
        new_mean = cls.calculate_new_mean(old_mean, old_n, new_data)
        old_variance = old_std ** 2
        new_variance = ((new_n - 2) * old_variance + (new_data - new_mean) * (new_data - old_mean)) / (new_n - 1)
        new_std = new_variance ** (1/2)
        return new_std
