import csv
import re

import digital_land


class HarmoniserPlugin:
    @digital_land.hookimpl
    def init_harmoniser_plugin(self, harmoniser):
        self.harmoniser = harmoniser

    @digital_land.hookimpl
    def apply_patch_post(self, fieldname, value):
        return value


harmoniser_plugin = HarmoniserPlugin()
