#!/usr/bin/env python
"""
Validator script for raft-level bright defects analysis.
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
    mask_file = '%s_bright_pixel_mask.fits' % wgSlotName
#    mask_file = '%s_bright_pixel_mask.fits' % sensor_id
    eotestUtils.addHeaderData(mask_file, LSST_NUM=sensor_id, TESTTYPE='DARK',
                              DATE=eotestUtils.utc_now_isoformat(),
                              CCD_MANU=ccd_vendor)
    results.append(siteUtils.make_fileref(mask_file, folder=slot))

    medianed_dark = '%s_median_dark_bp.fits' % wgSlotName
#    medianed_dark = '%s_median_dark_bp.fits' % sensor_id
    eotestUtils.addHeaderData(medianed_dark,
                              DATE=eotestUtils.utc_now_isoformat())
    results.append(siteUtils.make_fileref(medianed_dark, folder=slot))

    eotest_results = '%s_eotest_results.fits' % wgSlotName
#    eotest_results = '%s_eotest_results.fits' % sensor_id
    data = sensorTest.EOTestResults(eotest_results)
    amps = data['AMP']
    npixels = data['NUM_BRIGHT_PIXELS']
    ncolumns = data['NUM_BRIGHT_COLUMNS']
    for amp, npix, ncol in zip(amps, npixels, ncolumns):
        results.append(lcatr.schema.valid(lcatr.schema.get('bright_defects_raft'),
                                          amp=amp,
                                          bright_pixels=npix,
                                          bright_columns=ncol,
                                          slot=slot,
                                          sensor_id=sensor_id))
    # Persist the png files.
    metadata = dict(CCD_MANU=ccd_vendor, LSST_NUM=sensor_id,
                    TESTTYPE='DARK', TEST_CATEGORY='EO')
    results.extend(siteUtils.persist_png_files('%s*.png' % wgSlotName,
                                               sensor_id, folder=slot,
                                               metadata=metadata))

results.extend(siteUtils.jobInfo())
lcatr.schema.write_file(results)
lcatr.schema.validate_file()
