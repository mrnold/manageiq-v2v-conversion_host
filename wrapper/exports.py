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
import signal
import subprocess
import sys
import time


def export_nbd(port_map):
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/data/exports.log', 
        filemode='w')
    logger = logging.getLogger()
    stdout_handler = logging.StreamHandler(sys.stdout)
    stderr_handler = logging.StreamHandler(sys.stderr)
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    logging.info('Starting up, map is %s', str(port_map))

    # Start one nbdkit process per disk, using the port specified in the map
    processes = {}
    for disk, port in port_map.items():
        logging.info('Exporting %s over NBD, port %s', disk, str(port))
        cmd = [
            'nbdkit', '--exit-with-parent',
            '-p', str(port), 'file', disk
        ]
        processes[disk] = subprocess.Popen(cmd)

    # Check qemu-img info on all the disks to make sure everything is ready
    while True:
        try:
            for disk, port in port_map.items():
                cmd = ['qemu-img', 'info', 'nbd://localhost:{}'.format(port)]
                image_info = subprocess.check_output(cmd)
                logging.info('qemu-img info for %s:', disk, image_info)
        except Exception as error:
            logging.info('Got exception: %s', error)
            logging.info('Trying again.')
            time.sleep(1)
        else:
            logging.info('All volume exports ready.')
            break

    # Signal readiness (TODO)
    with open('/data/nbdready', 'w') as ready:
        ready.write('NBD exports ready')

    # Wait until told to stop
    signal.pause()
    logging.info('Got a stop signal, cleaning up...')
    for disk, process in processes.items():
        process.terminate()
        out, err = process.communicate()
        logging.info('Output from %s: %s', disk, out)
        logging.info('Errors from %s: %s', disk, err)
