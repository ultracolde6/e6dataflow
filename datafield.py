from datamodel import DataTool


class DataField(DataTool):
    def __init__(self, *, name):
        super(DataField, self).__init__(name=name, datatool_type=DataTool.SHOT_DATAFIELD)


class ShotDataField(DataField):
    def __init__(self, *, name):
        super(ShotDataField, self).__init__(name=name)

    def contains_shot(self, shot_num):
        raise NotImplementedError

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num, data):
        raise NotImplementedError


class DataStreamDataField(ShotDataField):
    def __init__(self, *, name, datastream_name, h5_subpath, h5_dataset_name):
        super(DataStreamDataField, self).__init__(name=name)
        self.datastream_name = datastream_name
        self.h5_subpath = h5_subpath
        self.h5_dataset_name = h5_dataset_name
        self.datastream = None

    def set_datamodel(self, datamodel):
        super(DataStreamDataField, self).set_datamodel(datamodel)
        self.datastream = datamodel.datatool_dict[self.datastream_name]

    def contains_shot(self, shot_num):
        return self.datastream.contains_shot(shot_num)

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


class DataDictShotDataField(ShotDataField):
    def __init__(self, *, name):
        super(DataDictShotDataField, self).__init__(name=name)
        self.datafield_dict = None

    def set_datamodel(self, datamodel):
        super(DataDictShotDataField, self).set_datamodel(datamodel)
        if self.name not in self.datamodel.data_dict['shot_data']:
            self.datamodel.data_dict['shot_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['shot_data'][self.name]

    def reset(self):
        self.datafield_dict = dict()

    def contains_shot(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        if shot_key in self.datafield_dict:
            return True
        else:
            return False

    def get_data(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        data = self.datafield_dict[shot_key]
        return data

    def set_data(self, shot_num, data):
        shot_key = f'shot_{shot_num:05d}'
        self.datafield_dict[shot_key] = data
