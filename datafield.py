from datamodel import InputParamLogger


class DataField(InputParamLogger):
    def __init__(self, *, datafield_name, datamodel):
        self.datafield_name = datafield_name
        self.datamodel = datamodel


class ShotDataField(DataField):
    def __init__(self, *, datafield_name, datamodel):
        super(ShotDataField, self).__init__(datafield_name=datafield_name, datamodel=datamodel)

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num):
        raise NotImplementedError


class DataStreamShotDataField(ShotDataField):
    def __init__(self, *, datafield_name, datamodel, datastream_name, h5_subpath, h5_dataset_name):
        super(DataStreamShotDataField, self).__init__(datafield_name=datafield_name, datamodel=datamodel)
        self.datastream_name = datastream_name
        self.h5_subpath = h5_subpath
        self.h5_dataset_name = h5_dataset_name
        self.datastream = datamodel.datastream_dict[datastream_name]

    def get_data(self, shot_num):
        h5_file = self.datastream.load_shot(shot_num)
        if self.h5_subpath is not None:
            h5_group = h5_file[self.h5_subpath]
        else:
            h5_group = h5_file
        data = h5_group[self.h5_dataset_name]
        return data

    def set_data(self, shot_num):
        raise UserWarning('Cannot set data in datastream datafields. These datafields represent raw data which should'
                          'not be manipulated')