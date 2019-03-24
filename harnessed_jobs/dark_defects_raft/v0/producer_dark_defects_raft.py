#!/usr/bin/env python
"""
Producer script for raft-level dark defects analysis.
"""
from __future__ import print_function
import lsst.cr-eotest.sensor as sensorTest
import os
import siteUtils
import eotestUtils
from multiprocessor_execution import sensor_analyses
import camera_components

def run_dark_pixels_task(sensor_id):
    print("run_dark_pixels_task: sensor_id = ",sensor_id)
#    raft_id = os.environ['LCATR_UNIT_ID']

#    raft = camera_components.Raft.create_from_etrav(raft_id)

#    wgSlotName = siteUtils.getWGSlotNames(raft)[sensor_id];

    "Single sensor execution of the dark pixels task."
    file_prefix = '%s_%s' % (sensor_id, siteUtils.getRunNumber())
    sflat_files = siteUtils.dependency_glob('S*/%s_sflat_500_flat_*.fits' % sensor_id,
                                            jobname=siteUtils.getProcessName('sflat_raft_acq'),
                                            description='Superflat files:')
    print("sflat query: ",'S*/%s_sflat_500_flat_H*.fits' % sensor_id)
    print("sflat_files = ",sflat_files)

    mask_files = \
        eotestUtils.glob_mask_files(pattern='%s_*mask.fits' % sensor_id)

    task = sensorTest.DarkPixelsTask()
    task.run(sensor_id, sflat_files, mask_files)

    siteUtils.make_png_file(sensorTest.plot_flat,
                            '%s_superflat_dark_defects.png' % file_prefix,
                            '%s_median_sflat.fits' % sensor_id,
                            title='%s, superflat for dark defects analysis' % sensor_id,
                            annotation='ADU/pixel')

if __name__ == '__main__':
    sensor_analyses(run_dark_pixels_task)
