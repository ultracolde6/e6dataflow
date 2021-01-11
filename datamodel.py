from enum import Enum
from pathlib import Path
import h5py
import pickle

class OverwriteMode(Enum):
    KEEP_OLD = 0
    KEEP_NEW = 1


class Reloadable:
    def __new__(cls, name, *args, **kwargs):
        input_param_dict = {'name':name, 'args': args, 'kwargs': kwargs, 'class': cls}
        object_data_dict = dict()
        rebuild_dict = {'input_param_dict':input_param_dict,
                        'object_data_dict':object_data_dict}
        obj = super(Reloadable, cls).__new__(cls)
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
        new_obj = rebuild_class.rebuild_object_data(new_obj, object_data_dict)
        return new_obj

    @staticmethod
    def rebuild_object_data(obj, object_data_dict):
        return obj

    def package_object_data(self):
        return self.rebuild_dict


class DataTool(Reloadable):
    DATASTREAM = 'datastream'
    PROCESSOR = 'processor'
    SHOT_DATAFIELD = 'shot_datafield'

    def __init__(self, *, name):
        self.updated = True

    @staticmethod
    def rebuild_object_data(obj, object_data_dict):
        obj.updated = object_data_dict['updated']
        
    def package_object_data(self):
        object_data_dict = self.rebuild_dict['object_data_dict']
        object_data_dict['updated'] = self.updated
        return self.rebuild_dict

    @classmethod
    def reload(cls, old_input_param_dict, old_tool_specific_data):
        new_datatool = Reloadable.rebuild(old_input_param_dict)
        cls.reload_tool_data(new_datatool=new_datatool, old_tool_specific_data=old_tool_specific_data)
        return new_datatool

    @staticmethod
    def reload_tool_data(new_datatool, old_tool_specific_data):
        pass

    def save_tool_data(self):
        pass


class DataModel(Reloadable):
    def __init__(self, *, daily_path, run_name, num_points, run_doc_string, overwrite_mode, quiet):
        self.daily_path = daily_path
        self.run_name = run_name
        self.num_points = num_points
        self.run_doc_string = run_doc_string
        self.overwrite_mode = overwrite_mode
        self.quiet = quiet

        self.num_shots = 0
        self.last_processed_shot = 0
        self.datamodel_file_path = Path(daily_path, run_name, 'datamodel.p')

        self.datastream_dict = dict()
        self.shot_datafield_dict = dict()
        self.processor_dict = dict()
        self.main_datastream = None

        self.data_dict = dict()

        self.data_dict[DataTool.DATASTREAM] = dict()
        self.data_dict[DataTool.SHOT_DATAFIELD] = dict()
        self.data_dict[DataTool.PROCESSOR] = dict()
        self.data_dict['datamodel'] = self.input_param_dict
        self.data_dict['shot_data'] = dict()

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
        datatool_name = datatool.name
        new_input_param_dict = datatool.rebuild_dict['input_param_dict']
        datatool_exists = datatool_name in target_container_dict
        if not datatool_exists:
            target_container_dict[datatool_name] = datatool
            datatool.updated = True
        elif datatool_exists:
            print(f'{datatool_type} "{datatool_name}" already exists in datamodel.')
            old_datatool = target_container_dict[datatool_name]
            old_input_param_dict = old_datatool.rebuild_dict['input_param_dict']
            if new_input_param_dict == old_input_param_dict:
                print(f'Old and new {datatool_type} have the same input paramaters')
            else:
                print(f'WARNING, old and new {datatool_type} differ.')
                if self.overwrite_mode is OverwriteMode.KEEP_OLD:
                    print(f'Using OLD {datatool_type} information. '
                          f'Change overwrite mode to update {datatool_type} parameters.')
                    old_datatool.updated = False
                elif self.overwrite_mode is OverwriteMode.KEEP_NEW:
                    print(f'Using NEW {datatool_type} information. '
                          f'Change overwrite mode to keep {datatool_type} parameters in the future.')
                    target_container_dict[datatool_name] = datatool
                    datatool.updated = True

    def create_datastream(self, datastream_name, file_prefix, set_main_datastream=False):
        datastream = DataStream(datastream_name=datastream_name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        self.add_datastream(datastream)

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

    @classmethod
    def load_datamodel(cls, daily_path, run_name):
        datamodel_file_path = Path(daily_path, run_name, 'datamodel.p')
        old_data_dict = pickle.load(open(datamodel_file_path, 'rb'))
        datamodel_input_param_dict = old_data_dict['datamodel']
        new_datamodel = InputParamLogger.rebuild(datamodel_input_param_dict)
        new_datamodel.num_shots = old_data_dict['num_shots']

    def save_datamodel(self):
        self.data_dict['num_shots'] = self.num_shots
        self.data_dict['last_processed_shot'] = self.last_processed_shot

        datamodel_file_path = Path(self.daily_path, self.run_name, 'datamodel.p')
        print(f'Saving data_dict to {datamodel_file_path}')
        pickle.dump(self.data_dict, open(datamodel_file_path, 'wb'))

    def package_object_data(self):
        object_data_dict = self.rebuild_dict['object_data_dict']
        object_data_dict['num_shots'] = self.num_shots
        object_data_dict['last_processed_shot'] = self.last_processed_shot

        object_data_dict['datastream'] = dict()
        for datastream in self.datastream_dict.values():
            object_data_dict['datastream'][datastream.name] = datastream.package_object_data()

        object_data_dict['shot_datafield'] = dict()
        for shot_datafield in self.shot_datafield_dict.values():
            object_data_dict['shot_datafield'][shot_datafield.name] = shot_datafield.package_object_data()

        object_data_dict['processor'] = dict()
        for processor in self.processor_dict.values():
            object_data_dict['processor'][processor.name] = processor.package_object_data()





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
