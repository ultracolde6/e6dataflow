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

    def __init__(self, *, name, datatool_type, parent_names):
        self.name = name
        self.datatool_type = datatool_type
        self.parent_names = parent_names
        if self.parent_names is not None:
            if isinstance(self.parent_names, str):
                self.parent_names = [parent_names]
            self.parent_names = set(self.parent_names)
        self.datamodel = None
        self.child_name_set = set()

    def reset(self):
        print(f'resetting {self.datatool_type}: {self.name}')
        for child_name in self.child_name_set:
            child = self.datamodel.datatool_dict[child_name]
            child.reset()

    def set_datamodel(self, datamodel):
        # set_datamodel must be called before link_within_datamodel
        self.datamodel = datamodel

    def link_within_datamodel(self):
        # set_datamodel must be called before link_within_datamodel
        if self.datamodel is None:
            raise ValueError('Datamodel must be set by set_datamodel before calling link_within_datamodel.')
        if self.parent_names is not None:
            for parent_name in self.parent_names:
                parent = self.datamodel.datatool_dict[parent_name]
                if self.name not in parent.child_name_set:
                    parent.child_name_set.add(self.name)


class ShotHandler(DataTool):
    def __init__(self, *, name, datatool_type, parent_names):
        super(ShotHandler, self).__init__(name=name, datatool_type=datatool_type, parent_names=parent_names)
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
