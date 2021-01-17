import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path
from datamodel import DataTool, ShotHandler
from utils import get_shot_list_from_point, shot_to_loop_and_point


class Reporter(DataTool):
    def __init__(self, name, reporter_type, save_data):
        super(Reporter, self).__init__(name=name, datatool_type=reporter_type)
        self.save_data = save_data
        self.save_path = None

    def link_within_datamodel(self):
        super(Reporter, self).link_within_datamodel()
        self.save_path = Path(self.datamodel.daily_path, 'analysis', self.datamodel.run_name, 'reporters', self.name)





class PointReporter(Reporter):
    def __init__(self, *, name, save_data):
        super().__init__(name=name, reporter_type=DataTool.POINT_REPORTER, save_data=save_data)
        self.fig_list = []

    def link_within_datamodel(self):
        super().link_within_datamodel()
        for point in range(self.datamodel.num_points):
            fig = plt.figure()
            self.fig_list.append(fig)
            fig.canvas.set_window_title(f'{self.name} - point_{point:02d}')

    def report(self):
        for point_num in range(self.datamodel.num_points):
            self.report_point(point_num)
            if self.save_data:
                self.save(point_num)

    def report_point(self, point_num):
        raise NotImplementedError

    def save(self, point_num):
        point_key = f'point_{point_num:02d}'
        file_name = f'{self.name} - {point_key}.png'
        file_path = Path(self.save_path, file_name)
        self.save_path.mkdir(parents=True, exist_ok=True)
        self.fig_list[point_num].savefig(file_path)


class ShotImageReporter(SingleShotReporter):
    def __init__(self, *, name, save_data, img_datafield_name_list, roi_dict):
        super(ShotImageReporter, self).__init__(name=name, save_data=save_data)
        self.img_datafield_name_list = img_datafield_name_list
        self.roi_dict = roi_dict
        self.num_img = len(self.img_datafield_name_list)
        self.ax_list = []
        self.imshow_list = []
        for img_num in range(self.num_img):
            ax = self.fig.add_subplot(1, self.num_img, img_num + 1)
            self.ax_list.append(ax)
            self.imshow_list.append(None)
        self.fig.set_size_inches(4 * self.num_img, 4)

    def _report(self, shot_num):
        vmin = np.inf
        vmax = - np.inf
        for img_num, datafield_name in enumerate(self.img_datafield_name_list):
            ax = self.ax_list[img_num]
            ax.clear()
            img = self.datamodel.get_data(datafield_name, shot_num)
            self.imshow_list[img_num] = ax.imshow(img)
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