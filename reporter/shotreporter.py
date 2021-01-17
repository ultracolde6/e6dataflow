import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path
from datatool import ShotHandler, DataTool
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
