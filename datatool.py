from .utils import qprint

class Rebuildable:
    def __new__(cls, *args, **kwargs):
        input_param_dict = {'args': args, 'kwargs': kwargs, 'class': cls}
        object_data_dict = dict()
        rebuild_dict = {'input_param_dict': input_param_dict,
                        'object_data_dict': object_data_dict}
        obj = super(Rebuildable, cls).__new__(cls)
        obj.rebuild_dict = rebuild_dict
        obj.input_param_dict = input_param_dict
        obj.object_data_dict = object_data_dict
        return obj

    @staticmethod
    def rebuild(rebuild_dict):
        input_param_dict = rebuild_dict['input_param_dict']
        rebuild_class = input_param_dict['class']
        rebuild_args = input_param_dict['args']
        rebuild_kwargs = input_param_dict['kwargs']
        new_obj = rebuild_class(*rebuild_args, **rebuild_kwargs)
        object_data_dict = rebuild_dict['object_data_dict']
        new_obj.rebuild_object_data(object_data_dict)
        new_obj.package_rebuild_dict()
        return new_obj

    def rebuild_object_data(self, object_data_dict):
        pass

    def package_rebuild_dict(self):
        pass


class DataTool(Rebuildable):
    DATASTREAM = 'datastream'
    PROCESSOR = 'processor'
    SHOT_DATAFIELD = 'shot_datafield'
    POINT_DATAFIELD = 'point_datafield'
    AGGREGATOR = 'aggregator'
    SINGLE_SHOT_REPORTER = 'single_shot_reporter'
    POINT_REPORTER = 'point_reporter'

    def __init__(self, *, name, datatool_type):
        self.name = name
        self.datatool_type = datatool_type
        self.datamodel = None
        self.child_list = []
        self.parent_list = []

    def reset(self):
        pass

    def set_datamodel(self, datamodel):
        self.datamodel = datamodel

    def link_within_datamodel(self):
        pass

    def add_child(self, child_datatool_name):
        if child_datatool_name not in self.child_list:
            self.child_list.append(child_datatool_name)
            child = self.datamodel.datatool_dict[child_datatool_name]
            child.add_parent(self.name)

    def add_parent(self, parent_datatool_name):
        if parent_datatool_name not in self.parent_list:
            self.parent_list.append(parent_datatool_name)
            parent = self.datamodel.datatool_dict[parent_datatool_name]
            parent.add_child(self.name)

    def get_descendents(self):
        descendent_list = []
        for descendent_name in self.child_list:
            descendent_list.append(descendent_name)
            descendent = self.datamodel.datatool_dict[descendent_name]
            descendent_list += descendent.get_descendents()
        return descendent_list


class ShotHandler(DataTool):
    def __init__(self, *, name, datatool_type):
        super(ShotHandler, self).__init__(name=name, datatool_type=datatool_type)
        self.handled_shots = []

    def reset(self):
        super(ShotHandler, self).reset()
        self.handled_shots = []

    def handle(self, shot_num, quiet=False):
        if shot_num not in self.handled_shots:
            qprint(f'handling shot {shot_num:05d} with "{self.name}" {self.datatool_type}', quiet)
            self._handle(shot_num)
            self.handled_shots.append(shot_num)
        else:
            qprint(f'skipping shot {shot_num:05d} with "{self.name}" {self.datatool_type}', quiet)

    def _handle(self, shot_num):
        raise NotImplementedError

    def package_rebuild_dict(self):
        super(ShotHandler, self).package_rebuild_dict()
        self.object_data_dict['handled_shots'] = self.handled_shots

    def rebuild_object_data(self, object_data_dict):
        super(ShotHandler, self).rebuild_object_data(object_data_dict)
        self.handled_shots = object_data_dict['handled_shots']
