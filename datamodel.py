from enum import Enum
from pathlib import Path
import h5py
import pickle


class OverwriteMode(Enum):
    KEEP_OLD = 0
    KEEP_NEW = 1


class Rebuildable:
    def __new__(cls, name, *args, **kwargs):
        input_param_dict = {'name':name, 'args': args, 'kwargs': kwargs, 'class': cls}
        object_data_dict = dict()
        rebuild_dict = {'input_param_dict':input_param_dict,
                        'object_data_dict':object_data_dict}
        obj = super(Rebuildable, cls).__new__(cls)
        obj.name = name
        obj.rebuild_dict = rebuild_dict
        return obj

    @staticmethod
    def rebuild(rebuild_dict):
        input_param_dict = rebuild_dict['input_param_dict']
        rebuild_class = input_param_dict['class']
        rebuild_name = input_param_dict['name']
        rebuild_args = input_param_dict['args']
        rebuild_kwargs = input_param_dict['kwargs']
        new_obj = rebuild_class(rebuild_name, *rebuild_args, **rebuild_kwargs)
        object_data_dict = rebuild_dict['object_data_dict']
        new_obj.rebuild_object_data(object_data_dict)
        return new_obj

    def rebuild_object_data(self, object_data_dict):
        pass

    def package_rebuild_dict(self):
        pass


class DataTool(Rebuildable):
    DATASTREAM = 'datastream'
    PROCESSOR = 'processor'
    SHOT_DATAFIELD = 'shot_datafield'

    def __init__(self, *, datatool_name):
        super(DataTool, self).__init__(name=datatool_name)
        self.updated = True
        self.datamodel = None

    def set_datamodel(self, datamodel):
        self.datamodel = datamodel

    def rebuild_object_data(self, object_data_dict):
        super(DataTool, self).rebuild_object_data(object_data_dict)
        self.updated = object_data_dict['updated']

    def package_rebuild_dict(self):
        super(DataTool, self).package_rebuild_dict()
        object_data_dict = self.rebuild_dict['object_data_dict']
        object_data_dict['updated'] = self.updated
        return self.rebuild_dict


class DataModel(Rebuildable):
    def __init__(self, *, datamodel_name, daily_path, run_name, num_points, run_doc_string,
                 overwrite_mode, quiet):
        super(DataModel, self).__init__(name=datamodel_name)
        self.daily_path = daily_path
        self.run_name = run_name
        self.num_points = num_points
        self.run_doc_string = run_doc_string
        self.overwrite_mode = overwrite_mode
        self.quiet = quiet

        self.num_shots = 0
        self.last_processed_shot = 0
        self.datamodel_file_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')

        self.datastream_dict = dict()
        self.shot_datafield_dict = dict()
        self.processor_dict = dict()
        self.main_datastream = None

        self.data_dict = dict()

    def run(self):
        self.get_num_shots()
        for shot_num in range(self.last_processed_shot, self.num_shots):
            print(f'** Processing shot_{shot_num:05d} **')
            self.process_data(shot_num)

    def get_num_shots(self):
        self.num_shots = self.main_datastream.count_shots()
        for datastream in self.datastream_dict.values():
            alternate_num_shots = datastream.count_shots()
            if alternate_num_shots != self.num_shots:
                raise UserWarning(f'num_shots from datastream "{datastream.datastream_name}" ({alternate_num_shots:d})'
                                  f' is not equal to num_shots from main datastream '
                                  f' "{self.main_datastream.datastream_name}" ({self.num_shots:d})')

    def process_data(self, shot_num):
        for processor in self.processor_dict.values():
            processor.process(shot_num=shot_num)

    def add_and_verify_datatool(self, datatool, target_container_dict, datatool_type):
        datatool.set_datamodel(datamodel=self)
        datatool_name = datatool.name
        datatool_exists = datatool_name in target_container_dict
        if not datatool_exists:
            target_container_dict[datatool_name] = datatool
            datatool.updated = True
        elif datatool_exists:
            print(f'{datatool_type} "{datatool_name}" already exists in datamodel.')
            old_datatool = target_container_dict[datatool_name]
            new_rebuild_dict = datatool.rebuild_dict
            old_rebuild_dict = old_datatool.rebuild_dict
            if new_rebuild_dict == old_rebuild_dict:
                print(f'Old and new {datatool_type} have the same input and stored data')
            else:
                print(f'WARNING, old and new {datatool_type} have different input and/or stored data.')
                if self.overwrite_mode is OverwriteMode.KEEP_OLD:
                    print(f'Using OLD {datatool_type}. '
                          f'Change overwrite mode to update {datatool_type} parameters.')
                    old_datatool.updated = False
                elif self.overwrite_mode is OverwriteMode.KEEP_NEW:
                    print(f'Using NEW {datatool_type}. '
                          f'Change overwrite mode to keep {datatool_type} parameters in the future.')
                    target_container_dict[datatool_name] = datatool
                    datatool.updated = True

    def create_datastream(self, datastream_name, file_prefix, set_main_datastream=False):
        datastream = DataStream(datastream_name=datastream_name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        self.add_datastream(datastream, set_main_datastream=set_main_datastream)

    def add_datastream(self, datastream, set_main_datastream=False):
        self.add_and_verify_datatool(datatool=datastream, target_container_dict=self.datastream_dict,
                                     datatool_type=DataTool.DATASTREAM)
        if set_main_datastream or self.main_datastream is None:
            self.main_datastream = datastream

    def add_shot_datafield(self, datafield):
        self.add_and_verify_datatool(datatool=datafield, target_container_dict=self.shot_datafield_dict,
                                     datatool_type=DataTool.SHOT_DATAFIELD)

    def add_processor(self, processor):
        self.add_and_verify_datatool(datatool=processor, target_container_dict=self.processor_dict,
                                     datatool_type=DataTool.PROCESSOR)

    def get_shot_data(self, shot_datafield_name, shot_num):
        shot_datafield = self.shot_datafield_dict[shot_datafield_name]
        data = shot_datafield.get_data(shot_num)
        return data

    def set_shot_data(self, shot_datafield_name, shot_num, data):
        shot_datafield = self.shot_datafield_dict[shot_datafield_name]
        shot_datafield.set_data(shot_num, data)

    @staticmethod
    def load_datamodel(daily_path, run_name):
        datamodel_file_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')
        rebuild_dict = pickle.load(open(datamodel_file_path, 'rb'))
        datamodel = Rebuildable.rebuild(rebuild_dict)
        return datamodel

    def save_datamodel(self):
        self.package_rebuild_dict()
        print(f'Saving datamodel with name "{self.name}" to {self.datamodel_file_path}')
        pickle.dump(self.rebuild_dict, open(self.datamodel_file_path, 'wb'))

    def package_rebuild_dict(self):
        super(DataModel, self).package_rebuild_dict()
        object_data_dict = self.rebuild_dict['object_data_dict']
        object_data_dict['num_shots'] = self.num_shots
        object_data_dict['last_processed_shot'] = self.last_processed_shot
        object_data_dict['datamodel_file_path'] = self.datamodel_file_path

        object_data_dict['datastream'] = dict()
        for datastream in self.datastream_dict.values():
            datastream.package_rebuild_dict()
            object_data_dict['datastream'][datastream.name] = datastream.rebuild_dict
        object_data_dict['main_datastream'] = self.main_datastream.name

        object_data_dict['shot_datafield'] = dict()
        for shot_datafield in self.shot_datafield_dict.values():
            shot_datafield.package_rebuild_dict()
            object_data_dict['shot_datafield'][shot_datafield.name] = shot_datafield.rebuild_dict

        object_data_dict['processor'] = dict()
        for processor in self.processor_dict.values():
            processor.package_rebuild_dict()
            object_data_dict['processor'][processor.name] = processor.rebuild_dict

        object_data_dict['data_dict'] = self.data_dict

    def rebuild_object_data(self, object_data_dict):
        super(DataModel, self).rebuild_object_data(object_data_dict)
        self.num_shots = object_data_dict['num_shots']
        self.last_processed_shot = object_data_dict['last_processed_shot']
        self.datamodel_file_path = object_data_dict['datamodel_file_path']

        for datastream_rebuild_dict in object_data_dict['datastream'].values():
            datastream = Rebuildable.rebuild(datastream_rebuild_dict)
            is_main_datastream = datastream.name == object_data_dict['main_datastream']
            self.add_datastream(datastream, set_main_datastream=is_main_datastream)

        for shot_datafield_rebuild_dict in object_data_dict['shot_datafield'].values():
            shot_datafield = Rebuildable.rebuild(shot_datafield_rebuild_dict)
            self.add_shot_datafield(shot_datafield)

        for processor_rebuild_dict in object_data_dict['processor'].values():
            processor = Rebuildable.rebuild(processor_rebuild_dict)
            self.add_processor(processor)

        self.data_dict = object_data_dict['data_dict']


class DataStream(DataTool):
    def __init__(self, *, datastream_name, daily_path, run_name, file_prefix):
        super(DataStream, self).__init__(datatool_name=datastream_name)
        self.datastream_name = datastream_name
        self.daily_path = daily_path
        self.run_name = run_name
        self.file_prefix = file_prefix
        self.data_path = Path(self.daily_path, 'data', run_name, self.datastream_name)

    def load_shot(self, shot_num):
        file_name = f'{self.file_prefix}_{shot_num:05d}.h5'
        file_path = Path(self.data_path, file_name)
        h5_file = h5py.File(file_path, 'r')
        return h5_file

    def count_shots(self):
        file_list = list(self.data_path.glob('*.h5'))
        num_shots = len(file_list)
        return num_shots
