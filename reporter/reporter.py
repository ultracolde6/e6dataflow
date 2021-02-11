import numpy as np
from pathlib import Path
from ..datatool import DataTool


class Reporter(DataTool):
    LAYOUT_HORIZONTAL = 'horizontal'
    LAYOUT_VERTICAL = 'vertical'
    LAYOUT_GRID = 'grid'

    def __init__(self, *, name, reporter_type, datafield_name_list, layout, save_data):
        super(Reporter, self).__init__(name=name, datatool_type=reporter_type)
        self.datafield_name_list = datafield_name_list
        self.layout = layout
        self.save_data = save_data

        self.num_datafields = len(self.datafield_name_list)
        self.num_rows, self.num_cols = get_layout(self.num_datafields, layout)
        self.save_path = None

    def link_within_datamodel(self):
        super(Reporter, self).link_within_datamodel()
        self.save_path = Path(Path.cwd(),'reporters',self.name)

def get_plot_limits(data_min, data_max, expansion_factor=1.1, min_lim=None, max_lim=None):
    range_span = data_max - data_min
    expanded_range = range_span * expansion_factor
    expanded_half_range = expanded_range / 2
    range_center = (data_max + data_min) / 2
    lower = range_center - expanded_half_range
    upper = range_center + expanded_half_range
    if min_lim is not None:
        lower = min_lim
    if max_lim is not None:
        upper = max_lim
    return lower, upper


def get_layout(num_plots, layout=Reporter.LAYOUT_GRID):
    if layout == Reporter.LAYOUT_GRID:
        nearest_square = np.ceil(num_plots ** (1 / 2))
        num_rows = np.ceil(num_plots / nearest_square)
        num_cols = nearest_square
    elif layout == Reporter.LAYOUT_HORIZONTAL:
        num_rows = 1
        num_cols = num_plots
    elif layout == Reporter.LAYOUT_VERTICAL:
        num_rows = num_plots
        num_cols = 1
    else:
        num_rows = 1
        num_cols = num_plots
    return num_rows, num_cols
