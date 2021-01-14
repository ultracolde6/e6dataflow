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
    AGGREGATOR = 'aggregator'
    VERIFIER = 'verifier'

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


def get_datamodel(*, daily_path, run_name, num_points, run_doc_string, quiet, overwrite_run_doc_string=False):
    try:
        datamodel = DataModel.load_datamodel(daily_path, run_name)
        if num_points != datamodel.num_points:
            raise ValueError(f'Specified num_points ({num_points}) does not match num_points for saved datamodel '
                             f'({num_points}). Changing num_points requires rebuilding the datamodel')
        if run_doc_string != datamodel.run_doc_string:
            print('Specified run_doc_string does not match saved run_doc_string')
            if overwrite_run_doc_string:
                print('Overwriting run_doc_string.')
                datamodel.run_doc_string = run_doc_string
        return datamodel
    except FileNotFoundError as e:
        print(e)
        print(f'Creating new datamodel')
        datamodel = DataModel(daily_path=daily_path, run_name=run_name, num_points=num_points,
                              run_doc_string=run_doc_string, quiet=quiet)
        return datamodel


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
        self.main_datastream = None

        self.data_dict = dict()
        self.data_dict['shot_data'] = dict()
        self.data_dict['point_data'] = dict()

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

    def get_aggregators(self):
        aggregator_list = []
        for datatool in self.datatool_dict.values():
            if datatool.datatool_type == DataTool.AGGREGATOR:
                aggregator_list.append(datatool)
        return aggregator_list

    def run(self, quiet=False):
        self.get_num_shots()
        for shot_num in range(self.last_processed_shot, self.num_shots):
            qprint(f'** Processing shot_{shot_num:05d} **', quiet=quiet)
            self.process_data(shot_num)
            # qprint(f'** Aggregating point_{point_num:d} **', quiet=quiet)
            self.aggregate_data(shot_num)
        self.save_datamodel()

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

    def aggregate_data(self, point_num):
        for aggregator in self.get_aggregators():
            aggregator.aggregate(shot_num=point_num)

    def set_datatool(self, datatool, overwrite=False):
        datatool_name = datatool.name
        datatool_type = datatool.datatool_type
        datatool_exists = datatool_name in self.datatool_dict
        if not datatool_exists:
            self.datatool_dict[datatool_name] = datatool
            datatool.set_datamodel(datamodel=self)
        elif datatool_exists:
            print(f'WARNING! {datatool_type} "{datatool_name}" already exists in datamodel.')
            old_datatool = self.datatool_dict[datatool_name]
            if datatool.input_param_dict == old_datatool.input_param_dict:
                print(f'OLD and NEW {datatool_type} have the same input parameters, using OLD {datatool_type}.')
            else:
                print(f'OLD and NEW {datatool_type} differ. overwrite set to {overwrite}')
                if overwrite:
                    print(f'Using NEW {datatool_type}.')
                    self.datatool_dict[datatool_name] = datatool
                    datatool.set_datamodel(datamodel=self)
                    print(f'Re-running the datamodel may result in overwriting datamodel data. ')
                    print(f'The following datatools are configured into reset mode:')
                    datatool.reset()
                    print(f'{datatool.datatool_type}: {datatool.name}')
                    for child_datatool_name in datatool.get_descendents():
                        child_datatool = self.datatool_dict[child_datatool_name]
                        child_datatool.reset()
                        print(f'{child_datatool.datatool_type}: {child_datatool.name}')
                elif not overwrite:
                    print(f'Using OLD {datatool_type}.')

    def create_datastream(self, name, file_prefix, set_main_datastream=False, overwrite=False):
        datastream = DataStream(name=name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        self.set_datastream(datastream, set_main_datastream=set_main_datastream, overwrite=overwrite)

    def set_datastream(self, datastream, set_main_datastream=False, overwrite=False):
        self.set_datatool(datatool=datastream, overwrite=overwrite)
        if set_main_datastream or self.main_datastream is None:
            self.main_datastream = datastream

    def set_shot_datafield(self, datafield, overwrite=False):
        self.set_datatool(datatool=datafield, overwrite=overwrite)

    def set_processor(self, processor, overwrite=False):
        self.set_datatool(datatool=processor, overwrite=overwrite)

    def get_data(self, datafield_name, shot_num):
        datafield = self.datatool_dict[datafield_name]
        data = datafield.get_data(shot_num)
        return data

    def set_data(self, datafield_name, shot_num, data):
        shot_datafield = self.datatool_dict[datafield_name]
        shot_datafield.set_data(shot_num, data)

    @staticmethod
    def load_datamodel(daily_path, run_name):
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

    def rebuild_object_data(self, object_data_dict):
        super(DataModel, self).rebuild_object_data(object_data_dict)
        self.num_shots = object_data_dict['num_shots']
        self.last_processed_shot = object_data_dict['last_processed_shot']
        self.datamodel_file_path = object_data_dict['datamodel_file_path']

        self.data_dict = object_data_dict['data_dict']

        for datatool_rebuild_dict in object_data_dict['datatools'].values():
            datatool = Rebuildable.rebuild(rebuild_dict=datatool_rebuild_dict)
            self.set_datatool(datatool, overwrite=False)

        self.main_datastream = self.datatool_dict[object_data_dict['main_datastream']]


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
