""" Block device exports

Methods for exporting block devices from a standalone wrapper instance. The UCI
is set up to immediately run virt-v2v-wrapper with JSON input, so the easiest
way to run a sub-process for nbdkit is to have that input trigger this module.
So if the JSON contains an 'nbd_export_only' object, the wrapper will just run
the export_nbd function.

The initial use case is migration from OpenStack, which has a UCI container
running inside a source migration VM. The migration destination wrapper will
connect to the migration source host and tell it to run the UCI container with
the nbd_export_only JSON, which will run a function in this module to manage
the actual nbdkit exports.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time

from .source_hosts import NBD_READY_SENTINEL, DEFAULT_TIMEOUT


def export_nbd(nbd_export_config):
    """
    Start one nbdkit process for every disk in the JSON input. Intended to be
    run standalone inside a source conversion host.
    """
    port_map = nbd_export_config['port_map']
    log_file = nbd_export_config['log_file']
    log_format = '%(asctime)s:%(levelname)s:' \
        + ' %(message)s (%(module)s:%(lineno)d)'
    logging.basicConfig(
        level=logging.DEBUG,
        filename=log_file,
        filemode='a',
        format=log_format)
    logging.info('Starting up, map is %s', str(port_map))

    # Start one nbdkit process per disk, using the port specified in the map
    processes = {}
    for disk, port in port_map.items():
        logging.info('Exporting %s over NBD, port %s', disk, str(port))
#       TODO! Get nbdkit file plugin installed on RHOSP conversion appliance!
#        cmd = ['nbdkit', '--exit-with-parent', '--ipaddr', '127.0.0.1',
#               '--port', str(port), 'file', disk]
#       Fall back to qemu-nbd for now
        cmd = ['qemu-nbd', '-p', str(port), '-b', '127.0.0.1', '--read-only',
               '--persistent', '--verbose', disk]
        processes[disk] = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)

    # Check qemu-img info on all the disks to make sure everything is ready
    logging.info('Waiting for valid qemu-img info on all exports...')
    pending_disks = set(port_map.keys())
    for second in range(DEFAULT_TIMEOUT):
        try:
            for disk in pending_disks.copy():
                port = port_map[disk]
                cmd = ['qemu-img', 'info', 'nbd://localhost:{}'.format(port)]
                image_info = subprocess.check_output(cmd)
                logging.info('qemu-img info for %s: %s', disk, image_info)
                pending_disks.remove(disk)
        except subprocess.CalledProcessError as error:
            logging.info('Got exception: %s', error)
            logging.info('Trying again.')
            time.sleep(1)
        else:
            logging.info('All volume exports ready.')
            break
    else:
        raise RuntimeError('Timed out starting nbdkit exports!')

    # Signal readiness by printing a status message to stdout. The wrapper on
    # the destination conversion host should be waiting for this status to
    # continue with the transfer.
    ready = json.dumps({'nbd_export_only': 'ready'})
    sys.stdout.write(ready)
    sys.stdout.write('\n')
    sys.stdout.flush()

    # Wait until told to stop
    signal.pause()
    logging.info('Got a stop signal, cleaning up...')
    for disk, process in processes.items():
        process.terminate()
        out, err = process.communicate()
        logging.info('Output from %s: %s', disk, out)
        logging.info('Errors from %s: %s', disk, err)
