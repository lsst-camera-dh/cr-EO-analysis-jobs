#!/usr/bin/env python
"""
Script to collect the EO analysis results for each sensor and write
an eotest_report.fits file.  Also create raft-level mosaics.
"""
from __future__ import print_function
import os
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt
import lsst.eotest.sensor as sensorTest
import lsst.eotest.raft as raftTest
from lcatr.harness.helpers import dependency_glob
import eotestUtils
import siteUtils
import camera_components
from tearing_detection import tearing_detection

def slot_dependency_glob(pattern, jobname):
    "Return an OrderedDict of files with the desired pattern, keyed by slot."
#    files = sorted(siteUtils.dependency_glob(os.path.join('S??', pattern),
    files = sorted(siteUtils.dependency_glob(pattern,
                                             jobname=jobname))
    print("slot_dependency_glob files = ",files," pattern = ", pattern," jobname = ",jobname," path = ",os.path.join('S??', pattern)  )

    slot = {}
    for fn in files :
        if "WREB0" in fn :
            slot[fn] = "ccd1"
        if "GREB0" in fn :
            slot[fn] = "guidesensor1"
        if "GREB1" in fn :
            slot[fn] = "guidesensor2"


#    return OrderedDict([(fn.split('/')[-2], fn) for fn in files])
    return OrderedDict([(slot[fn], fn) for fn in files])

run_number = siteUtils.getRunNumber()

# Use a mean bias file to determine the maximum number of active
# pixels for the image quality statistics.
bias_files = slot_dependency_glob('*_mean_bias_*.fits', 'fe55_raft_analysis')
total_num, rolloff_mask = sensorTest.pixel_counts(list(bias_files.values())[0])

# Exposure time (in seconds) for 95th percentile dark current shot
# noise calculation.
exptime = 15.

raft_id = siteUtils.getUnitId()
raft = camera_components.Raft.create_from_etrav(raft_id)
sensor_ids = {slot: sensor_id for slot, sensor_id in raft.items()}
summary_files = dependency_glob('summary.lims')
results_files = dict()
for slot, sensor_id in raft.items():
    wgSlotName = siteUtils.getWGSlotNames(raft)[sensor_id];

    # Aggregate information from summary.lims files into a final
    # EOTestResults output file for the desired sensor_id.
    repackager = eotestUtils.JsonRepackager()
    repackager.eotest_results.add_ccd_result('TOTAL_NUM_PIXELS', total_num)
    repackager.eotest_results.add_ccd_result('ROLLOFF_MASK_PIXELS',
                                             rolloff_mask)
    repackager.process_files(summary_files, sensor_id=wgSlotName)

    # Add 95th percentile dark current shot noise and add in quadrature
    # to read noise to produce updated total noise.
    shot_noise = repackager.eotest_results['DARK_CURRENT_95']*exptime
    total_noise = np.sqrt(repackager.eotest_results['READ_NOISE']**2
                          + shot_noise)
    for i, amp in enumerate(repackager.eotest_results['AMP']):
        repackager.eotest_results.add_seg_result(amp, 'DC95_SHOT_NOISE',
                                                 np.float(shot_noise[i]))
#        repackager.eotest_results['TOTAL_NOISE'][i] = total_noise[i]

    outfile = '%s_eotest_results.fits' % wgSlotName
    repackager.write(outfile)
    results_files[slot] = outfile

gains = dict()
for slot, res_file in results_files.items():
    results = sensorTest.EOTestResults(res_file)
    print("slot = ",slot," res_file = ",res_file," results = ",results)
    gains[slot] = dict([(amp, gain) for amp, gain
                        in zip(results['AMP'], results['GAIN'])])
    print("gains = ",gains)

title = '%s, %s' % (raft_id, run_number)
file_prefix = '%s_%s' % (raft_id, run_number)

# Raft-level mosaics of median darks, bias, superflats high and low.
dark_mosaic = raftTest.RaftMosaic(slot_dependency_glob('*median_dark_bp.fits',
                                                       'bright_defects_raft'),
                                  gains=gains)
dark_mosaic.plot(title='%s, medianed dark frames' % title,
                 annotation='e-/pixel, gain-corrected, bias-subtracted')
plt.savefig('%s_medianed_dark.png' % file_prefix)
del dark_mosaic

mean_bias = raftTest.RaftMosaic(bias_files, bias_subtract=False)
mean_bias.plot(title='%s, mean bias frames' % title, annotation='ADU/pixel')
plt.savefig('%s_mean_bias.png' % file_prefix)
del mean_bias

sflat_high = raftTest.RaftMosaic(slot_dependency_glob('*superflat_high.fits',
                                                      'cte_raft'), gains=gains)
sflat_high.plot(title='%s, high flux superflat' % title,
                annotation='e-/pixel, gain-corrected, bias-subtracted')
plt.savefig('%s_superflat_high.png' % file_prefix)
del sflat_high

sflat_low = raftTest.RaftMosaic(slot_dependency_glob('*superflat_low.fits',
                                                     'cte_raft'), gains=gains)
sflat_low.plot(title='%s, low flux superflat' % title,
               annotation='e-/pixel, gain-corrected, bias-subtracted')
plt.savefig('%s_superflat_low.png' % file_prefix)
del sflat_low

# QE images at 350, 500, 620, 750, 870, and 1000 nm.
for wl in (350, 500, 620, 750, 870, 1000):
    print("Processing %i nm image" % wl)
    files = slot_dependency_glob('*lambda_flat_%04i_*.fits' % wl,
                                 siteUtils.getProcessName('qe_raft_acq'))
    try:
        flat = raftTest.RaftMosaic(files, gains=gains)
        flat.plot(title='%s, %i nm' % (title, wl),
                  annotation='e-/pixel, gain-corrected, bias-subtracted')
        plt.savefig('%s_%04inm_flat.png' % (file_prefix, wl))
        del flat
    except IndexError as eobj:
        print(files)
        print(eobj)

# QE summary plot.
qe_summary_lims = \
    siteUtils.dependency_glob('summary.lims', jobname='qe_raft_analysis')[0]
qe_fig = raftTest.qe_summary_plot(qe_summary_lims, title=title)
plt.savefig('%s_QE_summary.png' % file_prefix)
del qe_fig

# Raft-level plots of read noise, nonlinearity, serial and parallel CTI,
# charge diffusion PSF, and gains from Fe55 and PTC.
spec_plots = raftTest.RaftSpecPlots(results_files)

#columns = 'READ_NOISE DC95_SHOT_NOISE TOTAL_NOISE'.split()
columns = 'READ_NOISE DC95_SHOT_NOISE'.split()
spec_plots.make_multi_column_plot(columns, 'noise per pixel (-e rms)', spec=9,
                                  title=title)
plt.savefig('%s_total_noise.png' % file_prefix)

try:
    spec_plots.make_plot('MAX_FRAC_DEV',
                         'non-linearity (max. fractional deviation)',
                         spec=0.03, title=title)
    plt.savefig('%s_linearity.png' % file_prefix)
except:
    print("Failed to produce linearity plot. Check specific analysis job")


spec_plots.make_multi_column_plot(('CTI_LOW_SERIAL', 'CTI_HIGH_SERIAL'),
                                  'Serial CTI (ppm)', spec=(5e-6, 3e-5),
                                  title=title, yscaling=1e6, yerrors=False,
                                  colors='br', ymax=4e-5)
plt.savefig('%s_serial_cti.png' % file_prefix)

spec_plots.make_multi_column_plot(('CTI_LOW_PARALLEL', 'CTI_HIGH_PARALLEL'),
                                  'Parallel CTI (ppm)', spec=3e-6,
                                  title=title, yscaling=1e6, yerrors=False,
                                  colors='br')
plt.savefig('%s_parallel_cti.png' % file_prefix)

spec_plots.make_plot('PSF_SIGMA', 'PSF sigma (microns)', spec=5., title=title,
                     ymax=5.2)
plt.savefig('%s_psf_sigma.png' % file_prefix)

#spec_plots.make_multi_column_plot(('GAIN', 'PTC_GAIN'), 'System Gain (e-/ADU)',
#                                  yerrors=True, title=title, colors='br')
spec_plots.make_multi_column_plot(('GAIN', 'GAIN'), 'System Gain (e-/ADU)',
                                  yerrors=True, title=title, colors='br')
plt.savefig('%s_system_gain.png' % file_prefix)

spec_plots.make_plot('DARK_CURRENT_95',
                     '95th percentile dark current (e-/pixel/s)',
                     spec=0.2, title=title)
plt.savefig('%s_dark_current.png' % file_prefix)
