import matplotlib.pyplot as plt
from pathlib import Path
from datatool import DataTool
from reporter.reporter import Reporter


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
