import matplotlib.pyplot as plt
from pathlib import Path
from datamodel import ShotHandler, DataTool
from reporter.reporter import Reporter
from utils import shot_to_loop_and_point


class SingleShotReporter(Reporter, ShotHandler):
    def __init__(self, *, name, save_data):
        super(SingleShotReporter, self).__init__(name=name, reporter_type=DataTool.SINGLE_SHOT_REPORTER,
                                                 save_data=save_data)
        self.fig = plt.figure()
        self.fig.canvas.set_window_title(self.name)

    def report(self, shot_num):
        self.handle(shot_num)

    def _handle(self, shot_num):
        self._report(shot_num)
        if self.save_data:
            self.save(shot_num)

    def _report(self, shot_num):
        raise NotImplementedError

    def save(self, shot_num):
        loop_num, point_num = shot_to_loop_and_point(shot_num, num_points=self.datamodel.num_points)
        loop_key = f'loop_{loop_num:05d}'
        point_key = f'point_{point_num:02d}'
        shot_key = f'shot_{shot_num:05d}'
        file_name = f'{self.name} - {loop_key} - {shot_key}.png'
        shot_save_path = Path(self.save_path, point_key)
        shot_save_path.mkdir(parents=True, exist_ok=True)
        file_path = Path(shot_save_path, file_name)
        self.fig.savefig(file_path, bbox_inches='tight')
