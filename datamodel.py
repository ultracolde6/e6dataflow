from enum import Enum
from pathlib import Path
import h5py
import pickle
from utils import qprint


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

    def __init__(self, *, name, datatool_type):
        self.name = name
        self.datatool_type = datatool_type
        self.datamodel = None
        self.child_list = []
        self.parent_list = []

    def set_datamodel(self, datamodel):
        self.datamodel = datamodel

    def set_child(self, child_datatool_name):
        pass

    def set_children_and_parents(self):
        pass


class DataModel(Rebuildable):
    class OverwriteMode(Enum):
        KEEP_OLD = 0
        KEEP_NEW = 1

    def __init__(self, *, daily_path, run_name, num_points, run_doc_string,
                 quiet):
        self.daily_path = daily_path
        self.run_name = run_name
        self.num_points = num_points
        self.run_doc_string = run_doc_string
        self.quiet = quiet

        self.num_shots = 0
        self.last_processed_shot = 0
        self.datamodel_file_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')

        self.datatool_dict = dict()
        # self.datastream_dict = dict()
        # self.shot_datafield_dict = dict()
        # self.processor_dict = dict()
        self.main_datastream = None

        self.data_dict = dict()
        self.data_dict['shot_data'] = dict()

    def get_datastreams(self):
        datastream_list = []
        for datatool in self.datatool_dict.values():
            if datatool.datatool_type == DataTool.DATASTREAM:
                datastream_list.append(datatool)
        return datastream_list

    def get_processors(self):
        processor_list = []
        for datatool in self.datatool_dict.values():
            if datatool.datatool_type == DataTool.PROCESSOR:
                processor_list.append(datatool)
        return processor_list

    def run(self, quiet=False):
        self.get_num_shots()
        for shot_num in range(self.last_processed_shot, self.num_shots):
            qprint(f'** Processing shot_{shot_num:05d} **', quiet=quiet)
            self.process_data(shot_num)

    def get_num_shots(self):
        self.num_shots = self.main_datastream.count_shots()
        for datastream in self.get_datastreams():
            alternate_num_shots = datastream.count_shots()
            if alternate_num_shots != self.num_shots:
                raise UserWarning(f'num_shots from datastream "{datastream.name}" ({alternate_num_shots:d}) '
                                  f'is not equal to num_shots from main datastream '
                                  f'"{self.main_datastream.name}" ({self.num_shots:d})')

    def process_data(self, shot_num):
        for processor in self.get_processors():
            processor.process(shot_num=shot_num)

    def add_datatool(self, datatool, overwrite=False):
        datatool_name = datatool.name
        datatool_type = datatool.dataatool_type
        datatool_exists = datatool_name in self.datatool_dict
        if not datatool_exists:
            self.datatool_dict[datatool_name] = datatool
            datatool.set_datamodel(datamodel=self)
        elif datatool_exists:
            print(f'WARNING! {datatool_type} "{datatool_name}" already exists in datamodel.')
            if overwrite:
                print(f'Using NEW {datatool_type}. '
                      f'Change overwrite mode to keep {datatool_type} parameters in the future.')
                self.datatool_dict[datatool_name] = datatool
                datatool.set_datamodel(datamodel=self)
            elif not overwrite:
                print(f'Using OLD {datatool_type}. '
                      f'Change overwrite mode to update {datatool_type} parameters.')

    def create_datastream(self, name, file_prefix, set_main_datastream=False, overwrite=False):
        datastream = DataStream(name=name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        self.add_datastream(datastream, set_main_datastream=set_main_datastream, overwrite=overwrite)

    def add_datastream(self, datastream, set_main_datastream=False, overwrite=False):
        self.add_datatool(datatool=datastream, overwrite=overwrite)
        if set_main_datastream or self.main_datastream is None:
            self.main_datastream = datastream

    def add_shot_datafield(self, datafield, overwrite=False):
        print(f'adding shot_datafield: {datafield.name}')
        self.add_datatool(datatool=datafield, overwrite=overwrite)

    def add_processor(self, processor, overwrite=False):
        self.add_datatool(datatool=processor, overwrite=overwrite)

    def get_shot_data(self, shot_datafield_name, shot_num):
        shot_datafield = self.datatool_dict[shot_datafield_name]
        data = shot_datafield.get_data(shot_num)
        return data

    def set_shot_data(self, shot_datafield_name, shot_num, data):
        shot_datafield = self.datatool_dict[shot_datafield_name]
        shot_datafield.set_data(shot_num, data)

    @staticmethod
    def load_datamodel(daily_path, run_name, reset=False):
        datamodel_file_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')
        print(f'Loading datamodel from {datamodel_file_path}')
        rebuild_dict = pickle.load(open(datamodel_file_path, 'rb'))
        datamodel = Rebuildable.rebuild(rebuild_dict)
        return datamodel

    def save_datamodel(self):
        self.package_rebuild_dict()
        print(f'Saving datamodel to {self.datamodel_file_path}')
        self.datamodel_file_path.parent.mkdir(parents=True, exist_ok=True)
        pickle.dump(self.rebuild_dict, open(self.datamodel_file_path, 'wb'))

    def package_rebuild_dict(self):
        super(DataModel, self).package_rebuild_dict()
        self.object_data_dict['num_shots'] = self.num_shots
        self.object_data_dict['last_processed_shot'] = self.last_processed_shot
        self.object_data_dict['datamodel_file_path'] = self.datamodel_file_path

        self.object_data_dict['datatools'] = dict()
        for datatool in self.datatool_dict.values():
            datatool.package_rebuild_dict()
            self.object_data_dict['datatools'][datatool.name] = datatool.rebuild_dict
        self.object_data_dict['main_datastream'] = self.main_datastream.name

        self.object_data_dict['data_dict'] = self.data_dict

        # self.object_data_dict['datastream'] = dict()
        # for datastream in self.datastream_dict.values():
        #     datastream.package_rebuild_dict()
        #     self.object_data_dict['datastream'][datastream.name] = datastream.rebuild_dict
        # self.object_data_dict['main_datastream'] = self.main_datastream.name
        #
        # self.object_data_dict['shot_datafield'] = dict()
        # for shot_datafield in self.shot_datafield_dict.values():
        #     shot_datafield.package_rebuild_dict()
        #     self.object_data_dict['shot_datafield'][shot_datafield.name] = shot_datafield.rebuild_dict
        #
        # self.object_data_dict['processor'] = dict()
        # for processor in self.processor_dict.values():
        #     processor.package_rebuild_dict()
        #     self.object_data_dict['processor'][processor.name] = processor.rebuild_dict

    def rebuild_object_data(self, object_data_dict):
        super(DataModel, self).rebuild_object_data(object_data_dict)
        self.num_shots = object_data_dict['num_shots']
        self.last_processed_shot = object_data_dict['last_processed_shot']
        self.datamodel_file_path = object_data_dict['datamodel_file_path']

        self.data_dict = object_data_dict['data_dict']

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


class DataStream(DataTool):
    def __init__(self, *, name, daily_path, run_name, file_prefix):
        super(DataStream, self).__init__(name=name, datatool_type=DataTool.DATASTREAM)
        self.daily_path = daily_path
        self.run_name = run_name
        self.file_prefix = file_prefix
        self.data_path = Path(self.daily_path, 'data', run_name, self.name)

    def contains_shot(self, shot_num):
        file_name = f'{self.file_prefix}_{shot_num:05d}.h5'
        file_path = Path(self.data_path, file_name)
        return file_path.exists()

    def load_shot(self, shot_num):
        file_name = f'{self.file_prefix}_{shot_num:05d}.h5'
        file_path = Path(self.data_path, file_name)
        h5_file = h5py.File(file_path, 'r')
        return h5_file

    def count_shots(self):
        file_list = list(self.data_path.glob('*.h5'))
        num_shots = len(file_list)
        return num_shots
