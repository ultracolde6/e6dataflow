from .datatool import DataTool


class DataField(DataTool):
    def __init__(self, *, name, datatool_type, parent_names):
        super(DataField, self).__init__(name=name, datatool_type=datatool_type, parent_names=parent_names)


class ShotDataField(DataField):
    def __init__(self, *, name, parent_names):
        super(ShotDataField, self).__init__(name=name, datatool_type=DataTool.SHOT_DATAFIELD,
                                            parent_names=parent_names)

    def get_data(self, shot_num):
        raise NotImplementedError

    def set_data(self, shot_num, data):
        raise NotImplementedError


class PointDataField(DataField):
    def __init__(self, *, name, parent_names):
        super(PointDataField, self).__init__(name=name, datatool_type=DataTool.POINT_DATAFIELD,
                                             parent_names=parent_names)

    def get_data(self, point_num):
        raise NotImplementedError

    def set_data(self, point_num, data):
        raise NotImplementedError


class DataStreamDataField(ShotDataField):
    def __init__(self, *, name, datastream_name, h5_subpath, h5_dataset_name, parent_names):
        super(DataStreamDataField, self).__init__(name=name, parent_names=parent_names)
        self.datastream_name = datastream_name
        self.h5_subpath = h5_subpath
        self.h5_dataset_name = h5_dataset_name
        self.datastream = None

    def link_within_datamodel(self):
        super(DataStreamDataField, self).link_within_datamodel()
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


class H5ShotDataField(ShotDataField):
    def __init__(self, *, name, parent_names):
        super(H5ShotDataField, self).__init__(name=name, parent_names=parent_names)
        self.datafield_group = None

    def link_within_datamodel(self):
        super(H5ShotDataField, self).link_within_datamodel()
        if self.name not in self.datamodel.data_h5['shot_data']:
            self.datamodel.data_h5['shot_data'].create_group(name=self.name)
        self.datafield_group = self.datamodel.data_h5['shot_data'][self.name]

    def reset(self):
        super(H5ShotDataField, self).reset()
        if self.name in self.datamodel.data_h5['shot_data']:
            del self.datamodel.data_h5['shot_data'][self.name]
        self.datafield_group = self.datamodel.data_h5['shot_data'].create_group(name=self.name)

    def get_data(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        data = self.datafield_group[shot_key][:].astype(float)
        return data

    def set_data(self, shot_num, data):
        shot_key = f'shot_{shot_num:05d}'
        if shot_key in self.datafield_group:
            del self.datafield_group[shot_key]
        self.datafield_group.create_dataset(name=shot_key, data=data)


class H5PointDataField(PointDataField):
    def __init__(self, *, name, parent_names):
        super(H5PointDataField, self).__init__(name=name, parent_names=parent_names)
        self.datafield_group = None

    def link_within_datamodel(self):
        super(H5PointDataField, self).link_within_datamodel()
        if self.name not in self.datamodel.data_h5['point_data']:
            self.datafield_group = self.datamodel.data_h5['point_data'].create_group(name=self.name)
        self.datafield_group = self.datamodel.data_h5['point_data'][self.name]

    def reset(self):
        super(H5PointDataField, self).reset()
        if self.name in self.datamodel.data_h5['point_data']:
            del self.datamodel.data_h5['point_data'][self.name]
        self.datafield_group = self.datamodel.data_h5['point_data'].create_group(name=self.name)

    def get_data(self, point_num):
        point_key = f'point_{point_num:02d}'
        data = self.datafield_group[point_key][:].astype(float)
        return data

    def set_data(self, point_num, data):
        point_key = f'point_{point_num:02d}'
        if point_key in self.datafield_group:
            del self.datafield_group[point_key]
        self.datafield_group[point_key] = data


class DataDictShotDataField(ShotDataField):
    def __init__(self, *, name, parent_names):
        super(DataDictShotDataField, self).__init__(name=name, parent_names=parent_names)
        self.datafield_dict = None

    def link_within_datamodel(self):
        super(DataDictShotDataField, self).link_within_datamodel()
        if self.name not in self.datamodel.data_dict['shot_data']:
            self.datamodel.data_dict['shot_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['shot_data'][self.name]

    def reset(self):
        super(DataDictShotDataField, self).reset()
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
    def __init__(self, *, name, parent_names):
        super(DataDictPointDataField, self).__init__(name=name, parent_names=parent_names)
        self.datafield_dict = None

    def link_within_datamodel(self):
        super(DataDictPointDataField, self).link_within_datamodel()
        if self.name not in self.datamodel.data_dict['point_data']:
            self.datamodel.data_dict['point_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['point_data'][self.name]
        
    def reset(self):
        super(DataDictPointDataField, self).reset()
        self.datamodel.data_dict['point_data'][self.name] = dict()
        self.datafield_dict = self.datamodel.data_dict['point_data'][self.name]

    def get_data(self, point_num):
        point_key = f'point_{point_num:d}'
        data = self.datafield_dict[point_key]
        return data

    def set_data(self, point_num, data):
        point_key = f'point_{point_num:d}'
        self.datafield_dict[point_key] = data
