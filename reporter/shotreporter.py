import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path
from datatool import ShotHandler, DataTool
from reporter.reporter import Reporter, get_shot_labels, get_plot_limits
from utils import get_data_min_max


class ShotReporter(Reporter, ShotHandler):
    def __init__(self, *, name, datafield_name_list, layout, save_data, min_lim=None, max_lim=None):
        super(ShotReporter, self).__init__(name=name, reporter_type=DataTool.SINGLE_SHOT_REPORTER,
                                           datafield_name_list=datafield_name_list, layout=layout,
                                           save_data=save_data)
        self.min_lim = min_lim
        self.max_lim = max_lim
        self.fig = plt.figure(self.name, figsize=(3 * self.num_cols, 3 * self.num_rows))
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
        self.fig.set_size_inches(3 * self.num_cols, 3 * self.num_rows)
        data_min = np.inf
        data_max = - np .inf
        for datafield_num, datafield_name in enumerate(self.datafield_name_list):
            ax = self.ax_list[datafield_num]
            ax.clear()
            data = self.datamodel.get_data(datafield_name, shot_num)
            new_plot = self._plot(ax, data)
            self.specific_plot_adjustments(ax, new_plot, datafield_name)
            try:
                self.plot_list[datafield_num] = new_plot
            except IndexError:
                self.plot_list.append(new_plot)
            new_min, new_max = get_data_min_max(data)
            data_min = min(data_min, new_min)
            data_max = max(data_max, new_max)
            ax.set_title(datafield_name)
        self.generic_plot_adjustments(data_min=data_min, data_max=data_max)
        shot_key, loop_key, point_key = get_shot_labels(shot_num, self.datamodel.num_points)
        self.fig.suptitle(f'{shot_key} - {loop_key} - {point_key}')
        self.fig.set_tight_layout({'rect': [0, 0.03, 1, 0.95]})
        plt.pause(0.005)

    def specific_plot_adjustments(self, ax, new_plot, datafield_name):
        pass

    def _plot(self, ax, data):
        raise NotImplementedError

    def generic_plot_adjustments(self, *, data_min, data_max, **kwargs):
        plot_min, plot_max = get_plot_limits(data_min, data_max, min_lim=self.min_lim, max_lim=self.max_lim)
        for data_plot in self.plot_list:
            self.generic_plot_adjustments_single(data_plot, data_min=plot_min, data_max=plot_max, **kwargs)

    def generic_plot_adjustments_single(self, data_plot, *, data_min, data_max, **kwargs):
        raise NotImplementedError

    def save(self, shot_num):
        shot_key, loop_key, point_key = get_shot_labels(shot_num, self.datamodel.num_points)
        file_name = f'{self.name} - {loop_key} - {shot_key} - {point_key} .png'
        shot_save_path = Path(self.save_path, point_key)
        shot_save_path.mkdir(parents=True, exist_ok=True)
        file_path = Path(shot_save_path, file_name)
        self.fig.savefig(file_path, bbox_inches='tight')


class ImageShotReporter(ShotReporter):
    def __init__(self, *, name, datafield_name_list, layout, save_data, roi_dict):
        super(ImageShotReporter, self).__init__(name=name, datafield_name_list=datafield_name_list, layout=layout,
                                                save_data=save_data)
        self.roi_dict = roi_dict

    def _plot(self, ax, data):
        new_plot = ax.imshow(data)
        return new_plot

    def generic_plot_adjustments_single(self, data_plot, *, data_min, data_max, **kwargs):
        data_plot.set_clim(vmin=data_min, vmax=data_max)

    def specific_plot_adjustments(self, ax, new_plot, datafield_name):
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
