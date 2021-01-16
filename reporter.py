import matplotlib.pyplot as plt
from pathlib import Path
from datamodel import DataTool
from utils import get_shot_list_from_point


class Reporter(DataTool):
    def __init__(self, *, name, reporter_type, save_data):
        super(Reporter, self).__init__(name=name, datatool_type=reporter_type)
        self.save_data = save_data
        self.save_path = None
        self.fig = plt.figure()

    def link_within_datamodel(self):
        super(Reporter, self).link_within_datamodel()
        self.save_path = Path(self.datamodel.daily_path, 'analysis', self.datamodel.run_name, 'reporters', self.name)


class SingleShotReporter(Reporter):
    def __init__(self, *, name, save_data):
        super(SingleShotReporter, self).__init__(name=name, reporter_type=DataTool.SINGLE_SHOT_REPORTER,
                                                 save_data=save_data)

    def report(self, shot_num):
        raise NotImplementedError

    def save(self, shot_num):
        shot_key = f'shot_{shot_num:05d}'
        file_name = f'{self.name} - {shot_key}'
        file_path = Path(self.save_path, file_name, '.png')
        self.fig.savefig(file_path)


class PointReporter(Reporter):
    def report(self):
        for point_num in range(self.datamodel.num_points):
            self.report_point(point_num)

    def report_point(self, point_num):
        raise NotImplementedError

    def save(self, point_num):
        point_key = f'point_{point_num:02d}'
        file_name = f'{self.name} - {point_key}'
        file_path = Path(self.save_path, file_name, '.png')
        self.fig.savefig(file_path)


class PlotReporter:
    def __init__(self, ymin=None, ymax=None, ylabel=None, title=None):
        self.ymin = ymin
        self.ymax = ymax
        self.ylabel = ylabel
        self.title = title

    def set_ax_ylims(self, ax, plot_data):
        data_min = min(plot_data)
        data_max = max(plot_data)
        data_center = (data_max + data_min) / 2
        data_half_range = data_max - data_min
        auto_range = [data_center - 1.1 * data_half_range, data_center + 1.1 * data_half_range]
        ylims = auto_range
        if self.ymin is not None:
            ylims[0] = self.ymin
        if self.ymax is not None:
            ylims[1] = self.ymax
        ax.set_ylim(ylims)

#
# class ShotImageReporter(SingleShotReporter):
#     def __init__(self, *, name, save_data, img_datafield_name):
#         super(ShotImageReporter, self).__init__(name=name, save_data=save_data)
#         self.img_datafield_name
#     def report(self, shot_num):




class ShotReporter(PointReporter, PlotReporter):
    def __init__(self, name, input_datafield_name, single_plot=False, ymin=None, ymax=None,
                 ylabel=None, title=None, histogram=False):
        super(ShotReporter, self).__init__(name=name, ymin=ymin, ymax=ymax, ylabel=ylabel, title=title)
        self.input_datafield_name = input_datafield_name
        if self.ylabel is None:
            self.ylabel = self.input_datafield_name
        if self.title is None:
            self.title = self.input_datafield_name
        self.single_plot = single_plot
        self.histogram = histogram
        self.ax_list = []

    def report_single(self):
        all_plot_data = []
        ax = self.ax_list[0]
        for point in range(self.datamodel.num_points):
            shot_list, num_loops = get_shot_list_from_point(point, num_points=self.datamodel.num_points,
                                                            num_shots=self.datamodel.last_handled_shot)
            plot_data = []
            for shot in shot_list:
                data = self.datamodel.get_data(self.input_datafield_name, shot)
                plot_data.append(data)
            if not self.histogram:
                ax.plot(plot_data, '.', label=f'point {point}')
            else:
                ax.hist(plot_data, label=f'point {point}', alpha=0.6)
            all_plot_data += plot_data
        ax.set_ylabel(self.ylabel)
        ax.set_title(self.title)

        ax.legend()
        if not self.histogram:
            self.set_ax_lims(ax, all_plot_data)

    def report_multi(self):
        for point in range(self.datamodel.num_points):
            shot_list, num_loops = get_shot_list_from_point(point, num_points=self.datamodel.num_points,
                                                            num_shots=self.datamodel.last_handled_shot)
            plot_data = []
            for shot in shot_list:
                data = self.datamodel.get_data(self.input_datafield_name, shot)
                plot_data.append(data)
            ax = self.ax_list[point]
            if not self.histogram:
                ax.plot(plot_data, '.')
                self.set_ax_lims(ax, plot_data)
            else:
                ax.hist(plot_data)
            ax.set_title(f'{self.title} - point {point}')
            ax.set_ylabel(self.ylabel)

    def report(self, shot_num):
        if self.fig is None:
            self.make_plot()
        for ax in self.ax_list:
            ax.clear()
        if self.single_plot:
            self.report_single()
        else:
            self.report_multi()

    def make_plot(self):
        num_points = self.datamodel.num_points
        if self.single_plot:
            self.fig = plt.figure(figsize=[8, 4])
            ax = self.fig.add_subplot(1, 1, 1)
            self.ax_list = [ax]
        else:
            self.fig = plt.figure(figsize=[8, 4 * num_points])
            for point in range(num_points):
                ax = self.fig.add_subplot(num_points, 1, point + 1)
                self.ax_list.append(ax)
            self.fig.set_tight_layout(True)


class ImagePointReporter(Reporter):
    def __init__(self, name, input_datafield_name, single_figure=True):
        super(ImagePointReporter, self).__init__(name=name)
        self.input_datafield_name = input_datafield_name
        self.single_figure = single_figure
        self.initialized = False
        self.fig_list = []
        self.ax_list = []

    def report(self, shot_num):
        if not self.initialized:
            self.make_plot()
        for point in range(self.datamodel.num_points):
            ax = self.ax_list[point]
            ax.clear()
            data = self.datamodel.get_data(self.input_datafield_name, point)['mean']
            ax.imshow(data)

    def make_plot(self):
        num_points = self.datamodel.num_points
        if self.single_figure:
            fig = plt.figure(figsize=[4, 4 * num_points])
            for point in range(num_points):
                ax = fig.add_subplot(num_points, 1, point + 1)
                self.ax_list.append(ax)
            self.fig_list.append(fig)
        else:
            for point in range(num_points):
                fig = plt.figure(figsize=[8, 8])
                ax = fig.add_subplot(1, 1, 1)
                self.fig_list.append(fig)
                self.ax_list.append(ax)
        self.initialized = True
