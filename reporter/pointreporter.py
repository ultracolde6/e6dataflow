import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datatool import DataTool
from reporter.reporter import Reporter, get_plot_limits
from utils import get_data_min_max, get_shot_labels


class PointReporter(Reporter):
    def __init__(self, *, name, datafield_name_list, layout, save_data, min_lim_list=None, max_lim_list=None):
        super().__init__(name=name, reporter_type=DataTool.POINT_REPORTER,
                         datafield_name_list=datafield_name_list, layout=layout,
                         save_data=save_data)
        self.min_lim_list = min_lim_list
        self.max_lim_list = max_lim_list
        self.fig_list = []
        self.ax_dict = dict()
        self.plot_dict = dict()

    def link_within_datamodel(self):
        super().link_within_datamodel()
        for point_num in range(self.datamodel.num_points):
            point_key = f'point_{point_num:02d}'
            fig = plt.figure(f'{self.name} - {point_key}', figsize=(3 * self.num_cols, 3 * self.num_rows))
            self.fig_list.append(fig)
            ax_list = []
            for ax_num in range(self.num_datafields):
                ax = fig.add_subplot(self.num_rows, self.num_cols, ax_num + 1)
                ax_list.append(ax)
            self.ax_dict[point_key] = ax_list
            plot_list = []
            self.plot_dict[point_key] = plot_list

    def report(self):
        for point_num in range(self.datamodel.num_points):
            self.report_point(point_num)
            if self.save_data:
                self.save(point_num)

    def report_point(self, point_num):
        point_key = f'point_{point_num:02d}'
        fig = self.fig_list[point_num]
        ax_list = self.ax_dict[point_key]
        fig.set_size_inches(3 * self.num_cols, 3 * self.num_rows)
        data_min = np.inf
        data_max = - np .inf
        plot_list = self.plot_dict[point_key]
        for datafield_num, datafield_name in enumerate(self.datafield_name_list):
            ax = ax_list[datafield_num]
            ax.clear()
            data = self.datamodel.get_data(datafield_name, point_num)
            new_plot = self._plot(ax, data)
            self.specific_plot_adjustments(ax, new_plot, datafield_name, point_num)
            try:
                plot_list[datafield_num] = new_plot
            except IndexError:
                plot_list.append(new_plot)
            new_min, new_max = get_data_min_max(data)
            data_min = min(data_min, new_min)
            data_max = max(data_max, new_max)
            ax.set_title(datafield_name)
        self.generic_plot_adjustments(point_num, data_min=data_min, data_max=data_max)
        fig.suptitle(f'{point_key}')
        fig.set_tight_layout({'rect': [0, 0.03, 1, 0.95]})
        plt.pause(0.005)

    def _plot(self,ax, data):
        raise NotImplementedError

    def specific_plot_adjustments(self, ax, new_plot, datafield_name, point_num):
        pass

    def generic_plot_adjustments(self, point_num, *, data_min, data_max, **kwargs):
        point_key = f'point_{point_num:02d}'
        min_lim = self.min_lim_list[point_num]
        max_lim = self.max_lim_list[point_num]
        plot_min, plot_max = get_plot_limits(data_min, data_max, min_lim=min_lim, max_lim=max_lim)
        plot_list = self.plot_dict[point_key]
        for data_plot in plot_list:
            self.generic_plot_adjustments_single(data_plot, data_min=plot_min, data_max=plot_max, **kwargs)

    def generic_plot_adjustments_single(self, data_plot, *, data_min, data_max, **kwargs):
        raise NotImplementedError

    def save(self, point_num):
        point_key = f'point_{point_num:02d}'
        file_name = f'{self.name} - {point_key}.png'
        file_path = Path(self.save_path, file_name)
        self.save_path.mkdir(parents=True, exist_ok=True)
        self.fig_list[point_num].savefig(file_path, bbox_inches='tight')


class PlotAllShotReporter(PointReporter):
    def __init__(self, *, name, save_data, datafield_name_list, ymin=None, ymax=None):
        super(PlotAllShotReporter, self).__init__(name=name, save_data=save_data)
        self.datafield_name_list = datafield_name_list
        self.ymin = ymin
        self.ymax = ymax
        self.ax_dict = dict()

    def link_within_datamodel(self):
        super(PlotAllShotReporter, self).link_within_datamodel()
        num_datafields = len(self.datafield_name_list)
        for point_num in range(self.datamodel.num_points):
            point_key = f'point_{point_num:02d}'
            self.ax_dict[point_key] = dict()
            fig = self.fig_list[point_num]
            for idx, datafield_name in enumerate(self.datafield_name_list):
                self.ax_dict[point_key][datafield_name] = fig.add_subplot(1, num_datafields, idx+1)

    def report_point(self, point_num):
        point_key = f'point_{point_num:02d}'
        fig = self.fig_list[point_num]
        for datafield_name in self.datafield_name_list:
            data = self.datamodel.get_data_by_point(datafield_name, point_num)
            ax = self.ax_dict[point_key][datafield_name]
            ax.plot(data, '.')
            fig.canvas.draw()
