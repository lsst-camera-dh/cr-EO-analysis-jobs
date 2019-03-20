#!/usr/bin/env python
"""
Validator script for raft-level read noise analysis.
"""
from __future__ import print_function
import glob
import lsst.eotest.sensor as sensorTest
import lcatr.schema
import siteUtils
import eotestUtils
import camera_components

raft_id = siteUtils.getUnitId()
raft = camera_components.Raft.create_from_etrav(raft_id)

results = []
for slot, sensor_id in raft.items():
    if 'ccd2' in slot:
        continue
    ccd_vendor = sensor_id.split('-')[0].upper()
    wgSlotName = siteUtils.getWGSlotNames(raft)[sensor_id];

#    read_noise_file = '%s_eotest_results.fits' % sensor_id
    read_noise_file = '%s_eotest_results.fits' % wgSlotName

    data = sensorTest.EOTestResults(read_noise_file)
    amps = data['AMP']
    read_noise_data = data['READ_NOISE']
    system_noise_data = data['SYSTEM_NOISE']
    total_noise_data = data['TOTAL_NOISE']
    for amp, read_noise, system_noise, total_noise in zip(amps, read_noise_data,
                                                          system_noise_data,
                                                          total_noise_data):
        results.append(lcatr.schema.valid(lcatr.schema.get('read_noise_raft'),
                                          amp=amp, read_noise=read_noise,
                                          system_noise=system_noise,
                                          total_noise=total_noise,
                                          slot=slot,
                                          sensor_id=sensor_id))

#    fe55_acq_job_id = siteUtils.get_prerequisite_job_id('S*/%s_fe55_fe55_*.fits' % sensor_id,
    fe55_acq_job_id = siteUtils.get_prerequisite_job_id('S*/%s_fe55_fe55_*.fits' % wgSlotName,
                                                        jobname=siteUtils.getProcessName('fe55_raft_acq'))

#    files = glob.glob('%s_read_noise?*.fits' % sensor_id)
    files = glob.glob('%s_read_noise?*.fits' % wgSlotName)
    for fitsfile in files:
        eotestUtils.addHeaderData(fitsfile, LSST_NUM=sensor_id, TESTTYPE='FE55',
                                  DATE=eotestUtils.utc_now_isoformat(),
                                  CCD_MANU=ccd_vendor)

    data_products = [siteUtils.make_fileref(item, folder=slot)
                     for item in files]
    results.extend(data_products)

    # Persist the png files.
    metadata = dict(CCD_MANU=ccd_vendor, LSST_NUM=sensor_id,
                    TESTTYPE='FE55', TEST_CATEGORY='EO')
    results.extend(siteUtils.persist_png_files('%s*.png' % sensor_id,
                                               sensor_id, folder=slot,
                                               metadata=metadata))

# Persist the raft-level overscan correlation plot.
metadata = dict(LSST_NUM=raft_id, TESTTYPE='FE55', TEST_CATEGORY='EO')
results.extend(siteUtils.persist_png_files('%s*.png' % raft_id, raft_id,
                                           metadata=metadata))

results.extend(siteUtils.jobInfo())
lcatr.schema.write_file(results)
lcatr.schema.validate_file()
