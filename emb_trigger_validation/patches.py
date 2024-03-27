from functools import cache
from order import Dataset, DatasetInfo, Shift
from order.util import typed


@cache
def patch_order_dataset_info_n_events_n_files():

    # the original members
    _orig_n_events = DatasetInfo.n_events
    _orig_n_files = DatasetInfo.n_files

    # create typed objects equivalent to the original ones, but with the ability to also set the attribute value
    n_events = typed(fparse=_orig_n_events.fparse, setter=True, deleter=False, name=None)
    n_files = typed(fparse=_orig_n_files.fparse, setter=True, deleter=False, name=None)

    # patch the members
    DatasetInfo.n_events = n_events
    DatasetInfo.n_files = n_files


@cache
def patch_order_dataset_n_events_n_files():

    # the original members
    _orig_n_events = Dataset.n_events
    _orig_n_files = Dataset.n_files

    # additional method for setting n_events
    def _n_events_fset(self, value: int):
        self.info[Shift.NOMINAL].n_events = value

    # additional method for setting n_files
    def _n_files_fset(self, value: int):
        self.info[Shift.NOMINAL].n_files = value

    # create property objects equivalent to the original ones, but with the ability to also set the attribute value
    n_events = property(fget=_orig_n_events.fget, fset=_n_events_fset, fdel=None)
    n_files = property(fget=_orig_n_files.fget, fset=_n_files_fset, fdel=None)

    # patch the members
    Dataset.n_events = n_events
    Dataset.n_files = n_files


def apply_patches():
    patch_order_dataset_info_n_events_n_files()
    patch_order_dataset_n_events_n_files()
