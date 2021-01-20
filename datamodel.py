from enum import Enum
from pathlib import Path
import pickle
from .datatool import Rebuildable, DataTool
from .utils import qprint, get_shot_list_from_point, dict_compare


def get_datamodel(*, daily_path, run_name, num_points, run_doc_string, quiet, overwrite_run_doc_string=False):
    try:
        datamodel_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')
        datamodel = DataModel.load_datamodel(datamodel_path)
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


def load_datamodel(*, daily_path, run_name):
    datamodel_path = Path(daily_path, 'analysis', run_name, f'{run_name}-datamodel.p')
    datamodel = DataModel.load_datamodel(datamodel_path)
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
        self.last_handled_shot = 0
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

    def get_datatool_of_type(self, datatool_type):
        datatool_list = []
        for datatool in self.datatool_dict.values():
            if datatool.datatool_type == datatool_type:
                datatool_list.append(datatool)
        return datatool_list

    def run(self, quiet=False):
        self.get_num_shots()
        for shot_num in range(self.last_handled_shot, self.num_shots):
            qprint(f'** Processing shot_{shot_num:05d} **', quiet=quiet)
            self.process_data(shot_num)
            self.aggregate_data(shot_num)
            self.report_single_shot(shot_num)
            self.last_handled_shot = shot_num
        self.report_point_data()
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
        for processor in self.get_datatool_of_type(DataTool.PROCESSOR):
            processor.process(shot_num=shot_num)

    def aggregate_data(self, shot_num):
        for aggregator in self.get_datatool_of_type(DataTool.AGGREGATOR):
            aggregator.aggregate(shot_num=shot_num)

    def report_point_data(self):
        for point_reporter in self.get_datatool_of_type(DataTool.POINT_REPORTER):
            point_reporter.report()

    def report_single_shot(self, shot_num):
        for reporter in self.get_datatool_of_type(DataTool.SINGLE_SHOT_REPORTER):
            reporter.report(shot_num=shot_num)

    def add_datatool(self, datatool, overwrite=False):
        datatool_name = datatool.name
        datatool_type = datatool.datatool_type
        datatool_exists = datatool_name in self.datatool_dict
        if not datatool_exists:
            self.datatool_dict[datatool_name] = datatool
            datatool.set_datamodel(datamodel=self)
            self.last_handled_shot = 0
        elif datatool_exists:
            print(f'WARNING! {datatool_type} "{datatool_name}" already exists in datamodel.')
            old_datatool = self.datatool_dict[datatool_name]
            if dict_compare(datatool.input_param_dict, old_datatool.input_param_dict):
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
                    self.last_handled_shot = 0
                elif not overwrite:
                    print(f'Using OLD {datatool_type}.')
        if datatool.datatool_type == DataTool.DATASTREAM and self.main_datastream is None:
            self.main_datastream = self.datatool_dict[datatool_name]

    def link_datatools(self):
        for datatool in self.datatool_dict.values():
            datatool.link_within_datamodel()

    def get_data(self, datafield_name, data_index):
        datafield = self.datatool_dict[datafield_name]
        data = datafield.get_data(data_index)
        return data

    def get_data_by_point(self, datafield_name, point_num):
        shot_list, num_loops = get_shot_list_from_point(point_num, self.num_points, self.last_handled_shot)
        data_list = []
        for shot_num in shot_list:
            data = self.get_data(datafield_name, shot_num)
            data_list.append(data)
        return data_list

    def set_data(self, datafield_name, data_index, data):
        shot_datafield = self.datatool_dict[datafield_name]
        shot_datafield.set_data(data_index, data)

    @staticmethod
    def load_datamodel(datamodel_path):
        print(f'Loading datamodel from {datamodel_path}')
        rebuild_dict = pickle.load(open(datamodel_path, 'rb'))
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
        self.object_data_dict['last_handled_shot'] = self.last_handled_shot
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
        self.last_handled_shot = object_data_dict['last_handled_shot']
        self.datamodel_file_path = object_data_dict['datamodel_file_path']

        self.data_dict = object_data_dict['data_dict']

        for datatool_rebuild_dict in object_data_dict['datatools'].values():
            datatool = Rebuildable.rebuild(rebuild_dict=datatool_rebuild_dict)
            self.add_datatool(datatool, overwrite=False)

        self.main_datastream = self.datatool_dict[object_data_dict['main_datastream']]
        self.link_datatools()
