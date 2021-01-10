from datamodel import DataTool


class DataField(DataTool):
    def __init__(self, *, datafield_name, datamodel):
        super(DataField, self).__init__(datatool_name=datafield_name)
        self.datafield_name = datafield_name
        self.datamodel = datamodel


class ShotDataField(DataField):
    def __init__(self, *, datafield_name, datamodel):
        super(ShotDataField, self).__init__(datafield_name=datafield_name, datamodel=datamodel)

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num, data):
        raise NotImplementedError


class DataStreamDataField(ShotDataField):
    def __init__(self, *, datafield_name, datamodel, datastream_name, h5_subpath, h5_dataset_name):
        super(DataStreamDataField, self).__init__(datafield_name=datafield_name, datamodel=datamodel)
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

    def set_data(self, shot_num, data):
        raise UserWarning('Cannot set data in datastream datafields. These datafields represent raw data which should'
                          'not be manipulated')


class ProcessedDataField(ShotDataField):
    def __init__(self, *, datafield_name, datamodel):
        super(ProcessedDataField, self).__init__(datafield_name=datafield_name, datamodel=datamodel)
        self.datamodel.data_dict['shot_data'][self.datafield_name] = dict()
        self.datafield_dict = self.datamodel.data_dict['shot_data'][self.datafield_name]

    def get_data(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        data = self.datafield_dict[shot_key]
        return data

    def set_data(self, shot_num, data):
        shot_key = f'shot_{shot_num:05d}'
        self.datafield_dict[shot_key] = data
