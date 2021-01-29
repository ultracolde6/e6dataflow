from pathlib import Path
import datetime
import matplotlib.pyplot as plt
import pickle
from .datatool import Rebuildable, DataTool
from .utils import qprint, get_shot_list_from_point, dict_compare, get_shot_labels


def get_datamodel(*, daily_path, run_name, num_points, run_doc_string, overwrite_run_doc_string=False):
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
                              run_doc_string=run_doc_string)
        return datamodel


def load_datamodel(*, daily_path, run_name, datamodel_name='datamodel'):
    datamodel_path = Path(daily_path, run_name, f'{run_name}-datamodel.p')
    datamodel = DataModel.load_datamodel(datamodel_path)
    return datamodel


class DataModel(Rebuildable):
    """ DataModel orchestrates the flow of the data through the various DataTools contained within.

    Attributes
    __________
    name : str
        The name of the datamodel. Used to support multiple datamodels for a single run. Used in the filename for the
        saved datamodel pickle file.
    daily_path : pathlib.Path
        Analysis for all runs on a given day are saved in a daily folder. This is the path to that folder
    run_name : str
        Name of the run. Typically run0, run1, ...
    num_points : int
        Number of points in the run
    run_doc_string : str
        Human readable string with basic information about the run such as details about the parameters for the
        different points.
    num_shots : int
        Total number of shots recognized by the datamodel. This value is updated every time the datamodel run method
        is called by determining the number of .h5 files in the master DataStream raw data directory.
    last_handled_shot : int
        The number of the last shot which has been processed by the DataModel.
    recently_run : bool
        Flag raised after the DataModel is successfully run so to raise a message within the continuous_run loop
    datamodel_file_path : pathlib.Path
        Path where the DataModel pickle file will be saved.
    datatool_dict : dict
        dictionary of DataTool within the DataModel. DataTool objects are added to the DataModel via the
        add_datatool method. Keys are the DataTool names and values are the DataTools themselves. If one DataTool
        must access an attribute of another DataTool within the DataModel it typically does so via that datatool_dict
        of the DataModel. This means that DataTools cannot communicate until they are both added to the same DataModel.
    main_datastream : e6dataflow.datastream.DataStream
        A DataModel may have multiple DataStream objects. It is necessary to specify one of the DataStream objects as
        the main_datastream. This is the DataStream which will be used to calculated num_shots when the DataModel is
        run. Typically the num_shots recorded for each DataStream should be always be equivalent, but there is an edge
        case in which the DataModel is run in the middle of a given shot after some data has been saved but before other
        data has been saved. For this reason it is best to set the datamodel which is saved last within a shot to be
        the main_datastream, however, the code may function properly even without this provision.
    data_dict : dict
        Nested dictionary containing procesed and aggregated data. Top level keys are 'shot_data' and 'point_data'.
        ShotDataDictDataFields point to data_dict['shot_data'] which is itself a dictionary and PointDataDictDataFields
        point to data_dict['point_data']. The dict at data_dict['<shot/point>_data'] contains more dicts named by the
        corresponding datafield name. These dicts contain data indexed by their shot or point number. For example:
        data_dict['shot_data'][<datafield_name>]['shot_00035']
        data_dict['point_data'][<datafield_name>]['point_03']
        each point to a particular piece of data.
        The shot_data and point_data dictionaries are initialized as empty and are subsequently formed by the
        DataField objects after they are added to the DataModel.

    Methods
    _______

    """
    def __init__(self, *, name='datamodel', daily_path, run_name, num_points, run_doc_string):
        self.name = name
        self.daily_path = daily_path
        self.run_name = run_name
        self.num_points = num_points
        self.run_doc_string = run_doc_string

        self.num_shots = 0
        self.last_handled_shot = -1
        self.recently_run = False
        self.datamodel_file_path = Path(self.daily_path, self.run_name, f'{self.run_name}-{self.name}.p')

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

    def run_continuously(self, quiet=False, handler_quiet=False, save_every_shot=False):
        print('Begining continuous running of datamodel.')
        while True:
            self.run(quiet=quiet, handler_quiet=handler_quiet, save_every_shot=save_every_shot)
            if self.recently_run:
                shot_key, loop_key, point_key = get_shot_labels(self.last_handled_shot + 1, self.num_points)
                time_string = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f'{time_string} -- .. Waiting for data: {shot_key} - {loop_key} - {point_key} ..')
                self.recently_run = False
            plt.pause(0.01)

    def run(self, quiet=False, handler_quiet=False, force_run=False, save_every_shot=False):
        self.get_num_shots()
        if self.last_handled_shot + 1 == self.num_shots and not force_run:
            return
        for shot_num in range(self.last_handled_shot + 1, self.num_shots):
            shot_key, loop_key, point_key = get_shot_labels(self.last_handled_shot + 1, self.num_points)
            time_string = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            qprint(f'{time_string} -- ** Processing {shot_key} - {loop_key} - {point_key} **', quiet=quiet)
            self.process_data(shot_num, quiet=handler_quiet)
            self.aggregate_data(shot_num, quiet=handler_quiet)
            self.report_single_shot(shot_num, quiet=handler_quiet)
            self.last_handled_shot = shot_num
            if save_every_shot:
                self.save_datamodel()
        self.report_point_data()
        self.save_datamodel()
        self.recently_run = True

    def get_num_shots(self):
        self.num_shots = self.main_datastream.count_shots()
        for datastream in self.get_datatool_of_type(DataTool.DATASTREAM):
            alternate_num_shots = datastream.count_shots()
            if alternate_num_shots != self.num_shots:
                raise UserWarning(f'num_shots from datastream "{datastream.name}" ({alternate_num_shots:d}) '
                                  f'is not equal to num_shots from main datastream '
                                  f'"{self.main_datastream.name}" ({self.num_shots:d})')

    def process_data(self, shot_num, quiet=False):
        for processor in self.get_datatool_of_type(DataTool.PROCESSOR):
            processor.process(shot_num=shot_num, quiet=quiet)

    def aggregate_data(self, shot_num, quiet=False):
        for aggregator in self.get_datatool_of_type(DataTool.AGGREGATOR):
            aggregator.aggregate(shot_num=shot_num, quiet=quiet)

    def report_point_data(self):
        for point_reporter in self.get_datatool_of_type(DataTool.POINT_REPORTER):
            point_reporter.report()

    def report_single_shot(self, shot_num, quiet=False):
        for reporter in self.get_datatool_of_type(DataTool.SINGLE_SHOT_REPORTER):
            reporter.report(shot_num=shot_num, quiet=quiet)

    def add_datatool(self, datatool, overwrite=False, rebuilding=False, quiet=False):
        datatool_name = datatool.name
        datatool_type = datatool.datatool_type
        datatool_exists = datatool_name in self.datatool_dict
        if not datatool_exists:
            self.datatool_dict[datatool_name] = datatool
            datatool.set_datamodel(datamodel=self)
            if not rebuilding:
                self.last_handled_shot = -1
        elif datatool_exists:
            qprint(f'WARNING! {datatool_type} "{datatool_name}" already exists in datamodel.', quiet)
            old_datatool = self.datatool_dict[datatool_name]
            if dict_compare(datatool.input_param_dict, old_datatool.input_param_dict):
                qprint(f'OLD and NEW {datatool_type} have the same input parameters, using OLD {datatool_type}.', quiet)
            else:
                qprint(f'OLD and NEW {datatool_type} differ. overwrite set to {overwrite}', quiet)
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
                    self.last_handled_shot = -1
                elif not overwrite:
                    qprint(f'Using OLD {datatool_type}.', quiet)
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
            self.add_datatool(datatool, overwrite=False, rebuilding=True, quiet=True)

        self.main_datastream = self.datatool_dict[object_data_dict['main_datastream']]
        self.link_datatools()
