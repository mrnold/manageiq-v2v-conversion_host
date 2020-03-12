""" Source hosts

Some migration sources require significant setup before they can export block
devices with nbdkit. This module holds the code used to set up nbdkit exports
on such sources.

For example, the current OpenStack export strategy is to shut down the source
VM, attach its volumes to a source conversion host, and export the volumes from
inside the conversion host via nbdkit. The OpenStackSourceHost class will take
care of the process up to this point, and some other class in the regular hosts
module will take care of copying the data from those exports to their final
migration destination. There is an exception for KVM-to-KVM migrations though,
because those aren't supposed to use virt-v2v - in this case, this module will
transfer the data itself instead of calling the main wrapper function.
"""

# TODO: change the container this module launches in the source conversion host
# TODO: provide transfer progress in state file
# TODO: build a list of cleanup actions
# TODO: check volume/snapshot quota up front
# TODO: handle direct launch from an image, should be able to create dest volume...
# TODO: name root volumes the same as source root, on destination side

import fcntl
import json
import logging
import openstack
import os
import pickle
import subprocess
import time
from collections import namedtuple

from .hosts import OpenstackHost
from .state import STATE

NBD_READY_SENTINEL = 'nbdready' # Created when nbdkit exports are ready
DEFAULT_TIMEOUT = 600           # Maximum wait for openstacksdk operations

# File containing ports used by all the nbdkit processes running on the source
# conversion host. This is meant to be shared among running containers. There
# is a check to see if the port is available, but this should speed things up.
PORT_MAP_FILE = '/var/run/v2v-wrapper-ports'
PORT_LOCK_FILE = '/var/lock/v2v-wrapper-lock' # Lock for the port map

# Lock to serialize volume attachments. This helps prevent device path
# mismatches between the OpenStack SDK and /dev in the VM.
ATTACH_LOCK_FILE = '/var/lock/v2v-volume-lock'

def detect_source_host(data, agent_sock):
    """ Create the right source host object based on the input data. """
    if 'osp_source_environment' in data:
        return OpenStackSourceHost(data, agent_sock)
    return None

class _BaseSourceHost(object):
    """ Interface for source hosts. """

    def prepare_exports(self):
        """ Creates the nbdkit exports from the migration source. """
        logging.info('No preparation needed for this migration source.')

    def close_exports(self):
        """ Stops the nbdkit exports on the migration source. """
        logging.info('No cleanup needed for this migration source.')

    def transfer_exports(self, host):
        """ Performs a data copy to a destination host. """
        logging.info('No transfer ability for this migration source.')

    def avoid_wrapper(self, host):
        """ Decide whether or not to avoid running virt-v2v. """
        logging.info('No reason to avoid virt-v2v from this migration source.')
        return True

    def get_disk_ids(self):
        """
        Get the list of volumes ready for a new VM, in the format expected by
        the disk_ids list in STATE.internal.
        """
        logging.info('This migration source provides no disk list.')
        return {}


VolumeMapping = namedtuple('VolumeMapping',
    ['source_dev', # Device path (like /dev/vdb) on source conversion host
     'source_id', # Volume ID on source conversion host
     'dest_dev', # Device path on destination conversion host
     'dest_id', # Volume ID on destination conversion host
     'snap_id', # Root volumes need snapshot+new volume, so record snapshot ID
     'size', # Volume size reported by OpenStack, in GB
     'url' # Final NBD export address from source conversion host
    ])
class OpenStackSourceHost(_BaseSourceHost):
    """ Export volumes from an OpenStack instance. """

    def __init__(self, data, agent_sock):
        # Create a connection to the source cloud
        osp_arg_list = ['auth_url', 'username', 'password',
                        'project_name', 'project_domain_name',
                        'user_domain_name', 'verify']
        osp_env = data['osp_source_environment']
        osp_args = {arg: osp_env[arg] for arg in osp_arg_list}
        self.source_converter = osp_env['conversion_vm_id']
        self.source_instance = osp_env['vm_id']
        self.conn = openstack.connect(**osp_args)

        # Create a connection to the destination cloud
        osp_arg_list = ['os-auth_url', 'os-username', 'os-password',
                        'os-project_name', 'os-project_domain_name',
                        'os-user_domain_name']
        osp_env = data['osp_environment']
        osp_args = {arg[3:]: osp_env[arg] for arg in osp_arg_list} # Trim 'os-'
        self.dest_converter = data['osp_server_id']
        if 'insecure_connection' in data:
            osp_args['verify'] = not data['insecure_connection']
        else:
            osp_args['verify'] = False
        self.dest_conn = openstack.connect(**osp_args)

        self.agent_sock = agent_sock
        self.exported = False
        openstack.enable_logging() # Lots of openstacksdk messages without this

        # Build up a list of VolumeMappings keyed by the original device path
        self.volume_map = {}

    def prepare_exports(self):
        """ Attach the source VM's volumes to the source conversion host. """
        self._test_ssh_connection()
        self._shutdown_source_vm()
        self._get_root_and_data_volumes()
        self._detach_data_volumes_from_source()
        self._attach_volumes_to_converter()
        self._export_volumes_from_converter()
        self.exported = True # TODO: something better than this

    def close_exports(self):
        """ Put the source VM's volumes back where they were. """
        if self.exported:
            self._test_ssh_connection()
            self._converter_close_exports()
            self._detach_volumes_from_converter()
            self._attach_data_volumes_to_source()
            self.exported = False

    def transfer_exports(self, host):
        self._create_destination_volumes()
        self._attach_destination_volumes()
        self._convert_destination_volumes()
        self._detach_destination_volumes()

    def avoid_wrapper(self, host):
        """ Assume OpenStack to OpenStack migrations are always KVM to KVM. """
        if isinstance(host, OpenstackHost):
            logging.info('OpenStack->OpenStack migration, skipping virt-v2v.')
            return True
        return False

    def _source_vm(self):
        """
        Changes to the VM returned by get_server_by_id are not necessarily
        reflected in existing objects, so just get a new one every time.
        """
        return self.conn.get_server_by_id(self.source_instance)

    def _converter(self):
        """ Same idea as _source_vm, for source conversion host. """
        return self.conn.get_server_by_id(self.source_converter)

    def _destination(self):
        """ Same idea as _source_vm, for destination conversion host. """
        return self.dest_conn.get_server_by_id(self.dest_converter)

    def _ssh_args(self):
        """ Provide default set of SSH options. """
        return [
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
        ]

    def _ssh_cmd(self, address, args):
        """ Build an SSH command and environment using the running agent. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        command = ['ssh']
        command.extend(self._ssh_args())
        command.extend(['cloud-user@'+address])
        command.extend(args)
        return command, environment

    def _converter_out(self, args):
        """ Run a command on the source conversion host and get the output. """
        address = self._converter().accessIPv4
        command, environment = self._ssh_cmd(address, args)
        output = subprocess.check_output(command, env=environment)
        return output.decode('utf-8').strip()

    def _converter_val(self, args):
        """ Run a command on the source conversion host and get return code """
        address = self._converter().accessIPv4
        command, environment = self._ssh_cmd(address, args)
        return subprocess.call(command, env=environment)

    def _converter_sub(self, args):
        """ Run a long-running command on the source conversion host. """
        address = self._converter().accessIPv4
        command, environment = self._ssh_cmd(address, args)
        return subprocess.Popen(command, env=environment)

    def _converter_scp(self, source, dest):
        """ Copy a file to the source conversion host. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        address = self._converter().accessIPv4
        command = ['scp']
        command.extend(self._ssh_args())
        command.extend([source, 'cloud-user@'+address+':'+dest])
        return subprocess.check_output(command, env=environment)

    def _test_ssh_connection(self):
        """ Quick SSH connectivity check for source conversion host. """
        out = self._converter_out(['echo conn'])
        if out.strip() == 'conn':
            return True
        return False

    def _shutdown_source_vm(self):
        """ Shut down the migration source VM before moving its volumes. """
        server = self.conn.compute.get_server(self._source_vm().id)
        if server.status != 'SHUTOFF':
            self.conn.compute.stop_server(server=server)
            logging.info('Waiting for source VM to stop...')
            self.conn.compute.wait_for_server(server, 'SHUTOFF',
                wait=DEFAULT_TIMEOUT)

    def _get_attachment(self, volume, vm):
        """
        Get the attachment object from the volume with the matching server ID.
        Convenience method for use only when the attachment is already certain.
        """
        for attachment in volume.attachments:
            if attachment.server_id == vm.id:
                return attachment
        raise RuntimeError('Volume is not attached to the specified instance!')

    def _get_root_and_data_volumes(self):
        """
        Volume mapping step one: get the IDs and sizes of all volumes on the
        source VM. Key off the original device path to eventually preserve this
        order on the destination.
        """
        sourcevm = self._source_vm()
        for server_volume in sourcevm.volumes:
            volume = self.conn.get_volume_by_id(server_volume['id'])
            logging.info('Inspecting volume: %s', volume.id)
            dev_path = self._get_attachment(volume, sourcevm).device
            self.volume_map[dev_path] = VolumeMapping(source_dev=None,
                source_id=volume.id, dest_dev=None, dest_id=None,
                snap_id=None, size=volume.size, url=None)

    def _detach_data_volumes_from_source(self):
        """
        Detach data volumes from source VM, and pretend to "detach" the boot
        volume by creating a new volume from a snapshot of the VM. Volume map
        step two: replace boot disk ID with this new volume's ID, and record
        snapshot ID for later deletion.
        """
        sourcevm = self._source_vm()
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            if path == '/dev/vda':
                # Create a snapshot of the root volume
                logging.info('Creating root device snapshot')
                root_snapshot = self.conn.create_volume_snapshot(force=True,
                    wait=True, name='rhosp-migration-{}'.format(volume_id),
                    volume_id=volume_id, timeout=DEFAULT_TIMEOUT)

                # Create a new volume from the snapshot
                logging.info('Creating new volume from root snapshot')
                root_volume_copy = self.conn.create_volume(wait=True,
                        name='rhosp-migration-{}'.format(volume_id),
                        snapshot_id=root_snapshot.id,
                        size=root_snapshot.size,
                        timeout=DEFAULT_TIMEOUT)

                # Update the volume map with the new volume ID
                self.volume_map[path] = mapping._replace(
                    source_id=root_volume_copy.id,
                    snap_id=root_snapshot.id)

            else: # Detach non-root volumes
                logging.info('Detaching %s from %s', volume.id, sourcevm.id)
                self.conn.detach_volume(server=sourcevm, volume=volume,
                    wait=True, timeout=DEFAULT_TIMEOUT)

    def _wait_for_volume_dev_path(self, conn, volume, vm, timeout):
        volume_id = volume.id
        for second in range(timeout):
            volume = conn.get_volume_by_id(volume_id)
            if volume.attachments:
                attachment = self._get_attachment(volume, vm)
                if attachment.device.startswith('/dev/'):
                    return
            time.sleep(1)
        raise RuntimeError('Timed out waiting for volume device path!')

    def _use_lock(lock_file):
        """ Boilerplate for functions that need to take a lock. """
        def _decorate_lock(function):
            def wait_for_lock(self):
                with open(lock_file, 'wb+') as lock:
                    for second in range(DEFAULT_TIMEOUT):
                        try:
                            logging.info('Waiting for lock %s...', lock_file)
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except OSError as error:
                            logging.info('Another conversion has the lock.')
                            time.sleep(1)
                    else:
                        raise RuntimeError('Unable to acquire lock %s!',
                            lock_file)
                    try:
                        function(self)
                    finally:
                        fcntl.flock(lock, fcntl.LOCK_UN)
            return wait_for_lock
        return _decorate_lock

    @_use_lock(ATTACH_LOCK_FILE)
    def _attach_volumes_to_converter(self):
        """ Attach all the source volumes to the conversion host """
        # Lock this part to have a better chance of the OpenStack device path
        # matching the device path seen inside the conversion host.
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s to conversion host...', volume_id)
            self.conn.attach_volume(server=self._converter(), volume=volume,
                wait=True, timeout=DEFAULT_TIMEOUT)
            self._wait_for_volume_dev_path(self.conn, volume,
                self._converter(), DEFAULT_TIMEOUT)

    def _test_port_available(self, port):
        """
        See if a port is open on the source conversion host by trying to listen
        on it.
        """
        result = self._converter_val(['timeout', '1', 'nc', '-l', str(port)])
        return result == 124 # Returned by 'timeout' when command timed out,
                             # meaning nc was successful and the port is free.


    @_use_lock(PORT_LOCK_FILE)
    def _export_volumes_from_converter(self):
        """
        SSH to source conversion host and start an NBD export. Start the UCI
        with /dev/vdb, /dev/vdc, etc. attached, then pass JSON input to request
        nbdkit exports from the V2V wrapper. Find free ports up front so they
        can be passed to the container. Volume mapping step 3: fill in the
        volume's device path on the source conversion host, and the URL to its
        NBD export.
        """
        logging.info('Exporting volumes from source conversion host...')

        # Create a temporary directory on source conversion host
        tmpdir = self._converter_out(['mktemp', '-d', '-t', 'v2v-XXXXXX'])
        logging.info('Source conversion host temp dir: %s', tmpdir)

        # Reserve ports on the source conversion host. Lock a file containing
        # the used ports, select some ports from the range that is unused, and
        # check that the port is available on the source conversion host. Add
        # this to the locked file and unlock it for the next conversion.
        if not os.path.exists(PORT_MAP_FILE):
            try:
                os.mknod(PORT_MAP_FILE)
            except FileExistsError as error: # Just in case multiple wrappers
                pass                         # try to do this at the same time.
        with open(PORT_MAP_FILE, 'rb+') as port_map_file:
            # Try to read in the set of used ports
            try:
                used_ports = pickle.load(port_map_file)
            except EOFError as error:
                logging.info('Unable to read port map from %s, re-initializing'
                    ' it...', port_map_file.name)
                used_ports = set()

            logging.info('Currently used ports: %s', str(used_ports))

            # Choose ports from the available possibilities, and try to bind
            ephemeral_ports = set(range(49152, 65535))
            available_ports = ephemeral_ports-used_ports

            # Expose and forward the ports we are going to tell nbdkit to use
            nbd_ports = []
            forward_ports = ['-N', '-T']
            device_list = []
            port_map = {}
            for path, mapping in self.volume_map.items():
                try:
                    port = available_ports.pop()
                    while not self._test_port_available(port):
                        logging.info('Port %d not available, trying another.')
                        used_ports.add(port) # Mark used to avoid trying again
                        port = available_ports.pop()
                except KeyError as error:
                    raise RuntimeError('No free ports on conversion host!')
                used_ports.add(port)
                logging.info('Allocated port %d, all used: %s', port,
                    str(used_ports))

                volume_id = mapping.source_id
                volume = self.conn.get_volume_by_id(volume_id)
                attachment = self._get_attachment(volume, self._converter())
                dev_path = attachment.device
                uci_dev_path = dev_path + '-v2v'
                logging.info('Exporting device %s...', dev_path)
                port_map[uci_dev_path] = port
                forward_ports.extend(['-L', '{0}:localhost:{0}'.format(port)])
                nbd_ports.extend(['-p', '{0}:{0}'.format(port)])
                device_list.extend(['--device', dev_path + ':' + uci_dev_path])
                self.volume_map[path] = mapping._replace(source_dev=dev_path,
                    url='nbd://localhost:'+str(port))
                logging.info('Volume map so far: %s', self.volume_map)

            # Get SSH to forward the NBD ports to localhost
            self.forwarding_process = self._converter_sub(forward_ports)

            # Copy the port map input
            self._converter_val(['mkdir', '-p', tmpdir+'/input'])
            ports = json.dumps({'nbd_export_only': port_map})
            nbd_conversion = '/tmp/nbd_conversion.json'
            with open(nbd_conversion, 'w+') as conversion:
                conversion.write(ports)
            self._converter_scp(nbd_conversion,
                tmpdir+'/input/conversion.json')

            # TODO: remove this, temporary update mechanism for development only
            self._converter_val(['mkdir', '-p', tmpdir+'/update'])
            self._converter_val(['cp', '/v2v/update/v2v-conversion-host.tar.gz',
                tmpdir+'/update/v2v-conversion-host.tar.gz'])
            self._converter_val(['cp', '/v2v2/update/v2v-conversion-host.tar.gz',
                tmpdir+'/update/v2v-conversion-host.tar.gz'])

            # Run UCI on source conversion host. Create a temporary directory
            # to use as the UCI's /data directory so more than one can run at a
            # time. TODO: change container name to the real thing
            ssh_args = ['sudo', 'podman', 'run', '--detach']
            ssh_args.extend(['-v', tmpdir+':/data:z'])
            ssh_args.extend(nbd_ports)
            ssh_args.extend(device_list)
            ssh_args.extend(['localhost/v2v-updater'])
            ssh_args.extend(['/usr/local/bin/entrypoint'])
            self.uci_id = self._converter_out(ssh_args)
            logging.debug('Source UCI container ID: %s', self.uci_id)

            # Make sure export worked by checking the exports. The conversion
            # host on the source should create an 'nbdready' file after it has
            # started all the nbdkit processes, and after that qemu-img info
            # should be able to read them.
            logging.info('Waiting for NBD exports from source container...')
            sentinel = os.path.join(tmpdir, NBD_READY_SENTINEL)
            for second in range(DEFAULT_TIMEOUT):
                if self._converter_val(['test', '-f', sentinel]) == 0:
                    break
                time.sleep(1)
            else:
                raise RuntimeError('Timed out waiting for NBD export!')
            for disk, port in port_map.items():
                cmd = ['qemu-img', 'info', 'nbd://localhost:{}'.format(port)]
                image_info = subprocess.check_output(cmd)
                logging.info('qemu-img info for %s: %s', disk, image_info)

            # Write out and unlock the port map
            logging.info('Writing out used ports: %s', str(used_ports))
            pickle.dump(used_ports, port_map_file)
            os.fsync(port_map_file)
            port_map_file.close()

            # TODO: Remove temporary directory from source conversion host
            # self._converter_out(['rm', '-rf', tmpdir])

    @_use_lock(PORT_LOCK_FILE)
    def _converter_close_exports(self):
        """
        SSH to source conversion host and close the NBD export. Currently this
        amounts to just stopping the container. (TODO) Also release the ports
        this process was using.
        """
        logging.info('Stopping export from source conversion host...')
        try:
            out = self._converter_out(['sudo', 'podman', 'stop', self.uci_id])
            logging.info('Closed NBD export with result: %s', out)
            self.forwarding_process.terminate()
            with open(PORT_MAP_FILE, 'rb+') as port_map_file:
                # Try to read in the set of used ports
                try:
                    used_ports = pickle.load(port_map_file)
                except EOFError as error:
                    logging.info('Unable to read port map file %s,'
                        ' re-initializing it...', port_map_file.name)
                    used_ports = set()

                logging.info('Cleanup: currently used ports: %s', used_ports)

                for path, mapping in self.volume_map:
                    port = int(mapping.url[-5:])
                    used_ports.remove(port)

                logging.info('Cleaning used ports: %s', used_ports)
                pickle.dump(used_ports, port_map_file)
                os.fsync(port_map_file)
                port_map_file.close()
        except Exception as error:
            pass

    def _volume_still_attached(self, volume, vm):
        """ Check if a volume is still attached to a VM. """
        for attachment in volume.attachments:
            if attachment.server_id == vm.id:
                return True
        return False

    @_use_lock(ATTACH_LOCK_FILE)
    def _detach_volumes_from_converter(self):
        """ Detach volumes from conversion host. """
        converter = self._converter()
        for path, mapping in self.volume_map.items():
            volume = self.conn.get_volume_by_id(mapping.source_id)
            logging.info('Inspecting volume %s', volume.id)
            attachment = self._get_attachment(volume, converter)
            if attachment.device == "/dev/vda":
                logging.info('This is the root volume for this VM')
            else:
                logging.info('This volume is a data disk for this VM')
                self.conn.detach_volume(server=converter, wait=True,
                    timeout=DEFAULT_TIMEOUT, volume=volume)
                for second in range(DEFAULT_TIMEOUT):
                    converter = self._converter()
                    volume = self.conn.get_volume_by_id(mapping.source_id)
                    if not self._volume_still_attached(volume, converter):
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError('Timed out waiting to detach volumes '
                        'from source conversion host!')

    def _attach_data_volumes_to_source(self):
        """ Clean up the copy of the root volume and reattach data volumes. """
        for path, mapping in sorted(self.volume_map.items()):
            if path == '/dev/vda':
                # Delete the temporary copy of the source root disk
                logging.info('Removing copy of root volume')
                self.conn.delete_volume(name_or_id=mapping.source_id,
                    wait=True, timeout=DEFAULT_TIMEOUT)

                # Remove the root volume snapshot
                logging.info('Deleting temporary root device snapshot')
                self.conn.delete_volume_snapshot(timeout=DEFAULT_TIMEOUT,
                    wait=True, name_or_id=mapping.snap_id)
            else:
                # Attach data volumes back to source VM
                logging.info('Re-attaching volumes to source VM...')
                volume = self.conn.get_volume_by_id(mapping.source_id)
                logging.info('Attaching %s back to source VM...', volume.id)
                self.conn.attach_volume(volume=volume, wait=True,
                    server=self._source_vm(), timeout=DEFAULT_TIMEOUT)

    def _create_destination_volumes(self): 
        """
        Volume mapping step 4: create new volumes on the destination OpenStack,
        and fill in dest_id with the new volumes.
        """
        logging.info('Creating volumes on destination cloud')
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            new_volume = self.dest_conn.create_volume(name=volume.name,
                bootable=volume.bootable, description=volume.description,
                size=volume.size, wait=True, timeout=DEFAULT_TIMEOUT)
            self.volume_map[path] = mapping._replace(dest_id=new_volume.id)

    def _attach_destination_volumes(self):
        """
        Volume mapping step 5: attach the new destination volumes to the
        destination conversion host. Fill in the destination device name.
        """
        logging.info('Attaching volumes to destination wrapper')
        for path, mapping in sorted(self.volume_map.items()):
            volume_id = mapping.dest_id
            volume = self.dest_conn.get_volume_by_id(volume_id)
            self.dest_conn.attach_volume(volume=volume, wait=True,
                server=self._destination(), timeout=DEFAULT_TIMEOUT)
            logging.info('Waiting for volume to appear in destination wrapper')
            self._wait_for_volume_dev_path(self.dest_conn, volume,
                self._destination(), DEFAULT_TIMEOUT)
            volume = self.dest_conn.get_volume_by_id(volume_id)
            dev_path = self._get_attachment(volume, self._destination()).device
            self.volume_map[path] = mapping._replace(dest_dev=dev_path)

    def _convert_destination_volumes(self):
        """
        Finally run the commands to copy the exported source volumes to the
        local destination volumes. Attempt to sparsify the volumes to minimize
        the amount of data sent over the network.
        """
        logging.info('Converting volumes...')
        for path, mapping in self.volume_map.items():
            logging.info('Converting source VM\'s %s: %s', path, str(mapping))
            overlay = '/tmp/'+os.path.basename(mapping.dest_dev)+'.qcow2'

            def _log_convert(source_disk, source_format):
                """ Write qemu-img convert progress to the wrapper log. """
                cmd = ['qemu-img', 'convert', '-p', '-f', source_format, '-O',
                        'host_device', source_disk, mapping.dest_dev]
                logging.info('Status from qemu-img:')
                with open(STATE.wrapper_log, 'a') as log_fd:
                    img_sub = subprocess.Popen(cmd,
                        stdout=log_fd,
                        stderr=subprocess.STDOUT,
                        stdin=subprocess.DEVNULL)
                    returncode = img_sub.wait()
                    logging.info('Conversion return code: %d', returncode)
                    if returncode != 0:
                        raise RuntimeError('Failed to convert volume!')

            try:
                logging.info('Attempting initial sparsify...')
                environment = os.environ.copy()
                environment['LIBGUESTFS_BACKEND'] = 'direct'

                cmd = ['qemu-img', 'create', '-f', 'qcow2', '-b', mapping.url,
                    overlay]
                out = subprocess.check_output(cmd)
                logging.info('Overlay output: %s', out)
                logging.info('Overlay size: %s', str(os.path.getsize(overlay)))

                cmd = ['virt-sparsify', '--in-place', overlay]
                out = subprocess.check_output(cmd, env=environment)
                logging.info('Sparsify output: %s', out)

                _log_convert(overlay, 'qcow2')
            except Exception as error:
                logging.info('Sparsify failed, converting whole device...')
                if os.path.isfile(overlay):
                    os.remove(overlay)
                _log_convert(mapping.url, 'raw')

    def _detach_destination_volumes(self):
        """ Disconnect new volumes from destination conversion host. """
        logging.info('Detaching volumes from destination wrapper.')
        for path, mapping in self.volume_map.items():
            volume_id = mapping.dest_id
            volume = self.dest_conn.get_volume_by_id(volume_id)
            self.dest_conn.detach_volume(volume=volume, wait=True,
                server=self._destination(), timeout=DEFAULT_TIMEOUT)

    def get_disk_ids(self):
        """ Return a list of disks as expected by STATE.internal's disk_ids """
        disk_ids = {}
        for path, mapping in self.volume_map.items():
            volume_id = mapping.dest_id
            disk_ids[path] = volume_id
        return disk_ids
