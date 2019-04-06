#!/usr/bin/env python
"""
Validator script for raft-level dark current analysis.
"""
import lsst.cr_eotest.sensor as sensorTest
import lcatr.schema
import siteUtils
import camera_components

raft_id = siteUtils.getUnitId()
raft = camera_components.Raft.create_from_etrav(raft_id)

results = []
for slot, sensor_id in raft.items():
    if 'ccd2' in slot :
        continue
    wgSlotName = siteUtils.getWGSlotNames(raft)[sensor_id]


    ccd_vendor = sensor_id.split('-')[0].upper()
    results_file = '%s_eotest_results.fits' % wgSlotName
    data = sensorTest.EOTestResults(results_file)

    amps = data['AMP']
    dc95s = data['DARK_CURRENT_95']
    for amp, dc95 in zip(amps, dc95s):
        results.append(lcatr.schema.valid(lcatr.schema.get('dark_current_raft'),
                                          amp=amp, dark_current_95CL=dc95,
                                          slot=slot,
                                          sensor_id=wgSlotName))

    # Persist the png files.
    metadata = dict(CCD_MANU=ccd_vendor, LSST_NUM=sensor_id,
                    TESTTYPE='DARK', TEST_CATEGORY='EO')
    results.extend(siteUtils.persist_png_files('%s*.png' % sensor_id,
                                               sensor_id, folder=slot,
                                               metadata=metadata))

results.extend(siteUtils.jobInfo())

lcatr.schema.write_file(results)
lcatr.schema.validate_file()
