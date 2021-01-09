from pathlib import Path
import h5py


class InputParamLogger:
    def __new__(cls, *args, **kwargs):
        input_param_dict = {'args': args, 'kwargs': kwargs, 'class': cls}
        obj = super(InputParamLogger, cls).__new__(cls)
        obj.input_param_dict = input_param_dict
        return obj

    @staticmethod
    def rebuild(input_param_dict):
        rebuild_class = input_param_dict['class']
        args = input_param_dict['args']
        kwargs = input_param_dict['kwargs']
        new_obj = rebuild_class(*args, **kwargs)
        return new_obj


class DataModel:
    def __init__(self, *, daily_path, run_name, run_doc_string, num_points, quiet):
        self.daily_path = daily_path
        self.run_name = run_name
        self.num_points = num_points
        self.run_doc_string = run_doc_string
        self.quiet = quiet

        self.data_dict = dict()
        self.data_dict['datastream'] = dict()

        self.datastream_dict = dict()
        self.shot_datafield_dict = dict()

    def check_datatool_existence(self, datatool, target_container_dict, datatool_type):
        datatool_name = datatool.datatool_name
        new_input_param_dict = datatool.input_param_dict
        datatool_exists = datatool_name in self.data_dict[datatool_type]
        if not datatool_exists:
            self.data_dict[datatool_type][datatool_name] = new_input_param_dict
            target_container_dict[datatool_name] = datatool
        else:
            print(f'{datatool_type} "{datatool_name}" already exists in datamodel.')
            old_input_param_dict = self.data_dict[datatool_type][datatool_name]
            if new_input_param_dict == old_input_param_dict:
                print(f'Old and new {datatool_type} have the same paramaters')
            else:
                print(f'WARNING, old and new {datatool_type} differ. Using old {datatool_type} information.'
                      f'Hard reset required to update {datatool_type} parameters.')

    def add_datastream(self, datastream_name, file_prefix):
        datastream = DataStream(datastream_name=datastream_name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        datastream_input_param_dict = datastream.input_param_dict
        datastream_exists = datastream_name in self.data_dict['datastreams']
        if not datastream_exists:
            self.data_dict['datastream'][datastream_name] = datastream_input_param_dict
            self.datastream_dict[datastream_name] = datastream
        else:
            print(f'datastream "{datastream_name}" already exists in datamodel.')
            new_input_param_dict = datastream_input_param_dict
            old_input_param_dict = self.data_dict['datastreams'][datastream_name]
            if new_input_param_dict == old_input_param_dict:
                print('Old and new datastreams have the same paramaters')
            else:
                print('WARNING, old and new datastreams differ. Using old datastream information.'
                      'Hard reset required to update datastream parameters.')

    def get_shot_data(self, shot_datafield_name, shot_num):
        shot_datafield = self.shot_datafield_dict[shot_datafield_name]
        data = shot_datafield.get_data[shot_num]
        return data

    def set_shot_data(self, shot_datafield_name, shot_num, data):
        shot_datafield = self.shot_datafield_dict[shot_datafield_name]
        shot_datafield.set_data(shot_num, data)


class DataTool(InputParamLogger):
    def __init__(self, *, datatool_name):
        self.datatool_name = datatool_name


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
