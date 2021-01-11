from datamodel import DataTool


class Aggregator(DataTool):
    def __init__(self, *, aggregator_name, datamodel):
        super(Aggregator, self).__init__(datatool_name=aggregator_name)
        self.aggregator_name = aggregator_name
        self.datamodel = datamodel

    def aggregate(self, shot_num):
        raise NotImplementedError


class AverageStdAggregator(Aggregator):
    def __init__(self, *, aggregator_name, datamodel):
        super(AverageStdAggregator, self).__init__(aggregator_name=aggregator_name, datamodel=datamodel)