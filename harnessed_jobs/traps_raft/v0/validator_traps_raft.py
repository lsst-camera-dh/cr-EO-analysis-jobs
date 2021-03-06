#!/usr/bin/env python
"""
Validator script for raft-level traps analysis.
"""
import lsst.cr_eotest.sensor as sensorTest
import lcatr.schema
import siteUtils
import eotestUtils
import camera_components

raft_id = siteUtils.getUnitId()
raft = camera_components.Raft.create_from_etrav(raft_id)

results = []
for slot, sensor_id in raft.items():
    if 'ccd2' in slot :
        continue
    wgSlotName = siteUtils.getWGSlotNames(raft)[sensor_id];


    ccd_vendor = sensor_id.split('-')[0].upper()

    trap_file = '%s_traps.fits' % wgSlotName
    eotestUtils.addHeaderData(trap_file, LSST_NUM=sensor_id, TESTTYPE='TRAP',
                              DATE=eotestUtils.utc_now_isoformat(),
                              CCD_MANU=ccd_vendor)
    results.append(siteUtils.make_fileref(trap_file, folder=slot))

    mask_file = '%s_traps_mask.fits' % wgSlotName
    results.append(siteUtils.make_fileref(mask_file, folder=slot))

    results_file = '%s_eotest_results.fits' % wgSlotName
    data = sensorTest.EOTestResults(results_file)
    amps = data['AMP']
    num_traps = data['NUM_TRAPS']

    for amp, ntrap in zip(amps, num_traps):
        results.append(lcatr.schema.valid(lcatr.schema.get('traps_raft'),
                                          amp=amp, num_traps=ntrap,
                                          slot=slot,
                                          sensor_id=wgSlotName))

results.extend(siteUtils.jobInfo())

lcatr.schema.write_file(results)
lcatr.schema.validate_file()
