import matplotlib.pyplot as plt
from datamodel import DataTool
from utils import get_shot_list_from_point


class Reporter(DataTool):
    def __init__(self, name):
        super(Reporter, self).__init__(name=name, datatool_type=DataTool.REPORTER)
        self.fig = None

    def report(self, shot_num):
        raise NotImplementedError


class PlotReporter(Reporter):
    def __init__(self, name, ymin=None, ymax=None, ylabel=None, title=None):
        super(PlotReporter, self).__init__(name=name)
        self.ymin = ymin
        self.ymax = ymax
        self.ylabel = ylabel
        self.title = title

    def report(self, shot_num):
        raise NotImplementedError

    def set_ax_lims(self, ax, plot_data):
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


class LoopReporter(PlotReporter):
    def __init__(self, name, input_datafield_name, single_plot=False, ymin=None, ymax=None,
                 ylabel=None, title=None, histogram=False):
        super(LoopReporter, self).__init__(name=name, ymin=ymin, ymax=ymax, ylabel=ylabel, title=title)
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
                                                            num_shots=self.datamodel.last_processed_shot)
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
                                                            num_shots=self.datamodel.last_processed_shot)
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
