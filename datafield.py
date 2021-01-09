from datamodel import InputParamLogger


class DataField(InputParamLogger):
    def __init__(self, *, datafield_name):
        self.datafield_name = datafield_name


class ShotDataField(DataField):
    def __init__(self, *, datafield_name):
        super(ShotDataField, self).__init__(datafield_name=datafield_name)

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num):
        raise NotImplementedError
