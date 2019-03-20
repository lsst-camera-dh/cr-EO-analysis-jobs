"""
Function to parallelize subcomponent analyses for an assembly such
as raft or full focal plane.
"""
import os
import multiprocessing
import siteUtils
import camera_components

__all__ = ['sensor_analyses']

def sensor_analyses(run_task_func, raft_id=None, processes=None):
    """
    Run a sensor-level analysis task implemented as a pickleable
    function that takes the desired sensor id as its single argument.

    Parameters
    ----------
    run_task_func : function
        A pickleable function that takes the sensor_id string as
        its argument.
    raft_id : str, optional
        The RTM (or RSA) LSST ID.  If None (default), the LCATR_UNIT_ID
        is used.
    processes : int, optional
        The maximum number of processes to have running at once.
        If None (default), then set to 1 or one less than
        the number of cores, whichever is larger.

    Notes
    -----
    Exceptions from subprocesses will be buffered until all of the
    subprocesses have finished running.  If any exceptions are thrown
    and are uncaught, a non-zero exit code will be generated for the
    overall process. Output to stdout or stderr from the subprocesses
    will be interleaved.

    Users can override the default or keyword argument values by setting
    the LCATR_PARALLEL_PROCESSES environment variable.
    """
    if raft_id is None:
        raft_id = siteUtils.getUnitId()

    if processes is None:
        processes = max(1, multiprocessing.cpu_count() - 1)
    processes = int(os.environ.get('LCATR_PARALLEL_PROCESSES', processes))

    print("raft_id = ",raft_id)

    raft = camera_components.Raft.create_from_etrav(raft_id)

#    print("sensor_id = ",raft.sensor_id)
    print("sensor_names = ",raft.sensor_names)
    print("slots = ",raft.slot_names)
    wgslot = siteUtils.getWGSlotNames(raft);
#    for slot,sensor_id in zip(raft.slot_names,raft.sensor_names):
#        if "ccd1" in slot or "ccd2" in slot :
#            wgslot[sensor_id] = 'WREB0'
#        elif "guidesensor1" in slot :
#            wgslot[sensor_id] = 'GREB0'
#        elif "guidesensor2" in slot  :
#            wgslot[sensor_id] = 'GREB1'

#    for sln = raft.slot_names :
#        if 'ccd1' in sln:
            
    print("wgslot = ",wgslot)

    if processes == 1:
        # For cases where only one process will be run at a time, it's
        # faster to run serially instead of using a
        # multiprocessing.Pool since the pickling that occurs can
        # cause significant overhead.
        for sensor_id in raft.sensor_names:
            run_task_func(wgslot[sensor_id])
    else:
        pool = multiprocessing.Pool(processes=processes)
        results = [pool.apply_async(run_task_func, (wgslot[sensor_id],))
                   for sensor_id in raft.sensor_names]
        pool.close()
        pool.join()
        for res in results:
            res.get()
