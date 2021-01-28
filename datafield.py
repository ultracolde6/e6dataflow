from .datatool import DataTool


class DataField(DataTool):
    def __init__(self, *, name, datatool_type):
        super(DataField, self).__init__(name=name, datatool_type=datatool_type)


class ShotDataField(DataField):
    def __init__(self, *, name):
        super(ShotDataField, self).__init__(name=name, datatool_type=DataTool.SHOT_DATAFIELD)

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num, data):
        raise NotImplementedError

    def get_data_list(self):
        data_list = []
        for shot_num in range(self.datamodel.last_handled_shot):
            data = self.get_data(shot_num)
            data_list.append(data)
        return data_list


class PointDataField(DataField):
    def __init__(self, *, name):
        super(PointDataField, self).__init__(name=name, datatool_type=DataTool.POINT_DATAFIELD)

    def get_data(self, point_num):
        raise NotImplementedError

    def set_data(self, point_num, data):
        raise NotImplementedError


class DataStreamDataField(ShotDataField):
    def __init__(self, *, name, datastream_name, h5_subpath, h5_dataset_name):
        super(DataStreamDataField, self).__init__(name=name)
        self.datastream_name = datastream_name
        self.h5_subpath = h5_subpath
        self.h5_dataset_name = h5_dataset_name
        self.datastream = None

    def link_tree_within_datamodel(self):
        super(DataStreamDataField, self).link_tree_within_datamodel()
        self.datastream = self.datamodel.datatool_dict[self.datastream_name]

    def get_data(self, shot_num):
        h5_file = self.datastream.load_shot(shot_num)
        if self.h5_subpath is not None:
            h5_group = h5_file[self.h5_subpath]
        else:
            h5_group = h5_file
        data = h5_group[self.h5_dataset_name][:].astype(float)
        return data

    def set_data(self, shot_num, data):
        raise UserWarning('Cannot set data in datastream datafields. These datafields represent raw data which should'
                          'not be manipulated')


class DataDictShotDataField(ShotDataField):
    def __init__(self, *, name):
        super(DataDictShotDataField, self).__init__(name=name)
        self.datafield_dict = None

    def reset(self):
        super(DataDictShotDataField, self).reset()
        self.datamodel.data_dict['shot_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['shot_data'][self.name]

    def link_tree_within_datamodel(self):
        super(DataDictShotDataField, self).link_tree_within_datamodel()
        if self.name not in self.datamodel.data_dict['shot_data']:
            self.datamodel.data_dict['shot_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['shot_data'][self.name]

    def get_data(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        data = self.datafield_dict[shot_key]
        return data

    def set_data(self, shot_num, data):
        shot_key = f'shot_{shot_num:05d}'
        self.datafield_dict[shot_key] = data


class DataDictPointDataField(PointDataField):
    def __init__(self, *, name):
        super(DataDictPointDataField, self).__init__(name=name)
        self.datafield_dict = None

    def reset(self):
        super(DataDictPointDataField, self).reset()
        self.datamodel.data_dict['point_data'][self.name] = dict()
        for point_num in range(self.datamodel.num_points):
            point_key = f'point_{point_num:d}'
            self.datamodel.data_dict['point_data'][self.name][point_key] = dict()
        self.datafield_dict = self.datamodel.data_dict['point_data'][self.name]

    def link_tree_within_datamodel(self):
        super(DataDictPointDataField, self).link_tree_within_datamodel()
        if self.name not in self.datamodel.data_dict['point_data']:
            self.datamodel.data_dict['point_data'][self.name] = dict()
            for point_num in range(self.datamodel.num_points):
                point_key = f'point_{point_num:d}'
                self.datamodel.data_dict['point_data'][self.name][point_key] = dict()
        self.datafield_dict = self.datamodel.data_dict['point_data'][self.name]

    def get_data(self, point_num):
        point_key = f'point_{point_num:d}'
        data = self.datafield_dict[point_key]
        return data

    def set_data(self, point_num, data):
        point_key = f'point_{point_num:d}'
        self.datafield_dict[point_key] = data
