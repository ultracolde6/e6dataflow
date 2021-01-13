from datamodel import DataTool


class Aggregator(DataTool):
    def __init__(self, *, name, verifier_list):
        super(Aggregator, self).__init__(name=name, datatool_type=DataTool.AGGREGATOR)
        self.verifier_list = verifier_list
        self.aggregated_shot_list = []
        self.num_aggregated_shots = 0

    def aggregate(self, shot_num):
        verified = True
        for verifier in self.verifier_list:
            if not verifier.verifier_list[shot_num]:
                verified = False
        if verified:
            self._aggregate(shot_num)
            self.aggregated_shot_list.append(shot_num)
            self.num_aggregated_shots = len(self.aggregated_shot_list)

    def _aggregate(self, shot_num):
        pass


class AverageStdAggregator(Aggregator):
    def __init__(self, *, name, verifier_list):
        super(AverageStdAggregator, self).__init__(name=name, verifier_list=verifier_list)
