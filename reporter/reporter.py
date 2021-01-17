import numpy as np
from pathlib import Path
from datatool import DataTool


class Reporter(DataTool):
    LAYOUT_HORIZONTAL = 'horizontal'
    LAYOUT_VERTICAL = 'vertical'
    LAYOUT_GRID = 'grid'

    def __init__(self, *, name, reporter_type, datafield_list, layout, save_data):
        super(Reporter, self).__init__(name=name, datatool_type=reporter_type)
        self.datafield_list = datafield_list
        self.layout = layout
        self.save_data = save_data

        self.num_datafields = len(self.datafield_list)
        self.num_rows, self.num_cols = get_layout(self.num_datafields, layout)
        self.save_path = None

    def link_within_datamodel(self):
        super(Reporter, self).link_within_datamodel()
        self.save_path = Path(self.datamodel.daily_path, 'analysis', self.datamodel.run_name, 'reporters', self.name)


def get_layout(num_plots, layout=Reporter.LAYOUT_GRID):
    if layout == Reporter.LAYOUT_GRID:
        nearest_square = np.ceil(num_plots ** (1 / 2))
        num_rows = np.ceil(num_plots / nearest_square)
        num_cols = nearest_square
    elif layout == Reporter.LAYOUT_HORIZONTAL:
        num_rows = num_plots
        num_cols = 1
    elif layout == Reporter.LAYOUT_VERTICAL:
        num_rows = 1
        num_cols = num_plots
    else:
        num_rows = 1
        num_cols = num_plots
    return num_rows, num_cols








#
#
#
# class ShotReporter(PointReporter):
#     def __init__(self, name, input_datafield_name, single_plot=False, ymin=None, ymax=None,
#                  ylabel=None, title=None, histogram=False):
#         super(ShotReporter, self).__init__(name=name, ymin=ymin, ymax=ymax, ylabel=ylabel, title=title)
#         self.input_datafield_name = input_datafield_name
#         if self.ylabel is None:
#             self.ylabel = self.input_datafield_name
#         if self.title is None:
#             self.title = self.input_datafield_name
#         self.single_plot = single_plot
#         self.histogram = histogram
#         self.ax_list = []
#
#     def report_single(self):
#         all_plot_data = []
#         ax = self.ax_list[0]
#         for point in range(self.datamodel.num_points):
#             shot_list, num_loops = get_shot_list_from_point(point, num_points=self.datamodel.num_points,
#                                                             num_shots=self.datamodel.last_handled_shot)
#             plot_data = []
#             for shot in shot_list:
#                 data = self.datamodel.get_data(self.input_datafield_name, shot)
#                 plot_data.append(data)
#             if not self.histogram:
#                 ax.plot(plot_data, '.', label=f'point {point}')
#             else:
#                 ax.hist(plot_data, label=f'point {point}', alpha=0.6)
#             all_plot_data += plot_data
#         ax.set_ylabel(self.ylabel)
#         ax.set_title(self.title)
#
#         ax.legend()
#         if not self.histogram:
#             self.set_ax_lims(ax, all_plot_data)
#
#     def report_multi(self):
#         for point in range(self.datamodel.num_points):
#             shot_list, num_loops = get_shot_list_from_point(point, num_points=self.datamodel.num_points,
#                                                             num_shots=self.datamodel.last_handled_shot)
#             plot_data = []
#             for shot in shot_list:
#                 data = self.datamodel.get_data(self.input_datafield_name, shot)
#                 plot_data.append(data)
#             ax = self.ax_list[point]
#             if not self.histogram:
#                 ax.plot(plot_data, '.')
#                 self.set_ax_lims(ax, plot_data)
#             else:
#                 ax.hist(plot_data)
#             ax.set_title(f'{self.title} - point {point}')
#             ax.set_ylabel(self.ylabel)
#
#     def report(self, shot_num):
#         if self.fig is None:
#             self.make_plot()
#         for ax in self.ax_list:
#             ax.clear()
#         if self.single_plot:
#             self.report_single()
#         else:
#             self.report_multi()
#
#     def make_plot(self):
#         num_points = self.datamodel.num_points
#         if self.single_plot:
#             self.fig = plt.figure(figsize=[8, 4])
#             ax = self.fig.add_subplot(1, 1, 1)
#             self.ax_list = [ax]
#         else:
#             self.fig = plt.figure(figsize=[8, 4 * num_points])
#             for point in range(num_points):
#                 ax = self.fig.add_subplot(num_points, 1, point + 1)
#                 self.ax_list.append(ax)
#             self.fig.set_tight_layout(True)
#
#
# class ImagePointReporter(Reporter):
#     def __init__(self, name, input_datafield_name, single_figure=True):
#         super(ImagePointReporter, self).__init__(name=name)
#         self.input_datafield_name = input_datafield_name
#         self.single_figure = single_figure
#         self.initialized = False
#         self.fig_list = []
#         self.ax_list = []
#
#     def report(self, shot_num):
#         if not self.initialized:
#             self.make_plot()
#         for point in range(self.datamodel.num_points):
#             ax = self.ax_list[point]
#             ax.clear()
#             data = self.datamodel.get_data(self.input_datafield_name, point)['mean']
#             ax.imshow(data)
#
#     def make_plot(self):
#         num_points = self.datamodel.num_points
#         if self.single_figure:
#             fig = plt.figure(figsize=[4, 4 * num_points])
#             for point in range(num_points):
#                 ax = fig.add_subplot(num_points, 1, point + 1)
#                 self.ax_list.append(ax)
#             self.fig_list.append(fig)
#         else:
#             for point in range(num_points):
#                 fig = plt.figure(figsize=[8, 8])
#                 ax = fig.add_subplot(1, 1, 1)
#                 self.fig_list.append(fig)
#                 self.ax_list.append(ax)
#         self.initialized = True


def expand_range(lims, expansion_factor, min_lim=None, max_lim=None):
    lower = lims[0]
    upper = lims[1]
    range_span = upper - lower
    expanded_range = range_span * expansion_factor
    expanded_half_range = expanded_range / 2
    range_center = (upper + lower) / 2
    new_range = [range_center - expanded_half_range, range_center + expanded_half_range]
    if min_lim is not None:
        new_range[0] = min_lim
    if max_lim is not None:
        new_range[1] = max_lim
    return new_range