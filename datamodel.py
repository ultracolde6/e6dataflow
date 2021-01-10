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

        self.datastream_dict = dict()
        self.shot_datafield_dict = dict()
        self.processor_dict = dict()

        self.data_dict = dict()
        self.data_dict['datastream'] = dict()
        self.main_datastream = None
        self.data_dict['shot_datafield'] = dict()
        self.data_dict['processor'] = dict()
        self.data_dict['shot_data'] = dict()

        self.num_shots = 0
        self.last_processed_shot = 0

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
                      f' Hard reset required to update {datatool_type} parameters.')

    def create_datastream(self, datastream_name, file_prefix, set_main_datastream=False):
        datastream = DataStream(datastream_name=datastream_name, daily_path=self.daily_path,
                                run_name=self.run_name, file_prefix=file_prefix)
        self.add_datastream(datastream)

    def add_datastream(self, datastream, set_main_datastream=False):
        self.add_and_verify_datatool(datatool=datastream, target_container_dict=self.datastream_dict,
                                     datatool_type='datastream')
        if set_main_datastream or self.main_datastream is None:
            self.main_datastream = datastream

    def add_shot_datafield(self, datafield):
        self.add_and_verify_datatool(datatool=datafield, target_container_dict=self.shot_datafield_dict,
                                     datatool_type='shot_datafield')

    def add_processor(self, processor):
        self.add_and_verify_datatool(datatool=processor, target_container_dict=self.processor_dict,
                                     datatool_type='processor')

    def get_shot_data(self, shot_datafield_name, shot_num):
        shot_datafield = self.shot_datafield_dict[shot_datafield_name]
        data = shot_datafield.get_data(shot_num)
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
