import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path
from datatool import ShotHandler, DataTool
from reporter.reporter import Reporter, get_shot_labels
from utils import shot_to_loop_and_point


class SingleShotReporter(Reporter, ShotHandler):
    def __init__(self, *, name, datafield_list, layout, save_data):
        super(SingleShotReporter, self).__init__(name=name, reporter_type=DataTool.SINGLE_SHOT_REPORTER,
                                                 datafield_list=datafield_list, layout=layout,
                                                 save_data=save_data)
        self.fig = plt.figure(self.name, figsize=(4 * self.num_rows, 4 * self.num_cols))
        self.ax_list = []
        self.plot_list = []
        for ax_num in range(self.num_datafields):
            ax = self.fig.add_subplot(self.num_rows, self.num_cols, ax_num + 1)
            self.ax_list.append(ax)

    def report(self, shot_num):
        self.handle(shot_num)

    def _handle(self, shot_num):
        self._report(shot_num)
        if self.save_data:
            self.save(shot_num)

    def _report(self, shot_num):
        data_min = np.inf
        data_max = - np .inf
        for datafield_num, datafield_name in enumerate(self.datafield_list):
            ax = self.ax_list[datafield_num]
            ax.clear()
            data = self.datamodel.get_data(datafield_name, shot_num)
            plot = self._plot(data)

    def _plot(self, data):
        raise NotImplementedError

    def save(self, shot_num):
        shot_key, loop_key, point_key = get_shot_labels(shot_num, self.datamodel.num_points)
        file_name = f'{self.name} - {point_key}- {loop_key} - {shot_key}.png'
        shot_save_path = Path(self.save_path, point_key)
        shot_save_path.mkdir(parents=True, exist_ok=True)
        file_path = Path(shot_save_path, file_name)
        self.fig.savefig(file_path, bbox_inches='tight')


class ShotImageReporter(SingleShotReporter):
    def __init__(self, *, name, datafield_list, layout, save_data, roi_dict):
        super(ShotImageReporter, self).__init__(name=name, datafield_list=datafield_list, layout=layout,
                                                save_data=save_data)
        self.roi_dict = roi_dict

    def _report(self, shot_num):
        vmin = np.inf
        vmax = - np.inf
        for datafield_num, datafield_name in enumerate(self.datafield_list):
            ax = self.ax_list[datafield_num]
            ax.clear()
            img = self.datamodel.get_data(datafield_name, shot_num)
            plot = ax.imshow(img)
            try:
                self.plot_list[datafield_num] = plot
            except IndexError:
                self.plot_list.append(plot)
            vmin = min(vmin, img.min())
            vmax = max(vmax, img.max())
            if datafield_name in self.roi_dict:
                roi_list = self.roi_dict[datafield_name]
                for roi in roi_list:
                    horizontal_slice = roi[1]
                    horizontal_span = horizontal_slice.stop - horizontal_slice.start
                    vertical_slice = roi[0]
                    vertical_span = vertical_slice.stop - vertical_slice.start
                    rect = Rectangle((horizontal_slice.start, vertical_slice.start), horizontal_span, vertical_span,
                                     linewidth=1, edgecolor='white', facecolor='none')
                    ax.add_patch(rect)

        for imshow_object in self.imshow_list:
            imshow_object.set_clim(vmin=vmin, vmax=vmax)
        loop_num, point_num = shot_to_loop_and_point(shot_num, num_points=self.datamodel.num_points)
        loop_key = f'loop_{loop_num:05d}'
        point_key = f'point_{point_num:02d}'
        shot_key = f'shot_{shot_num:05d}'
        self.fig.suptitle(f'{self.name} - {shot_key} - {point_key} - {loop_key}')
        self.fig.canvas.draw()
        plt.pause(0.005)
