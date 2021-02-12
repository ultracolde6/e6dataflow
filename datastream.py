from pathlib import Path
import h5py
from .datatool import DataTool


class DataStream(DataTool):
    def __init__(self, *, name, daily_path, run_name, file_prefix):
        super(DataStream, self).__init__(name=name, datatool_type=DataTool.DATASTREAM)
        self.daily_path = daily_path
        self.run_name = run_name
        self.file_prefix = file_prefix
        self.data_path = Path(self.daily_path, 'data', run_name, self.name)

    def load_shot(self, shot_num):
        file_name = f'{self.file_prefix}_{shot_num:05d}.h5'
        file_path = Path(self.data_path, file_name)
        h5_file = h5py.File(file_path, 'r')
        return h5_file

    def count_shots(self):
        # print('Looking for data in', self.data_path)
        file_list = list(self.data_path.glob('*.h5'))
        num_shots = len(file_list)
        return num_shots
