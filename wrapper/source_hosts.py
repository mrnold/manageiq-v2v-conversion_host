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

# TODO: rename to something more accurate than source_hosts, fix documentation
# TODO: change the container this module launches in the source conversion host
# TODO: formalize nbdready signal
# TODO: make sure ports are unused before proceeding (flock in source UCI)
# TODO: provide transfer progress in state file
# TODO: make sure to attach new volumes in sorted order
# TODO: wait for volume attachments instead of sleeping
# TODO: generate individual temporary directory
# TODO: remove need for root_volume_copy?
# TODO: document volume mapping buildup sequence
# TODO: build a list of cleanup actions
# TODO: clarify source vm, source/destination conversion hosts, and dest vm
# TODO: check volume/snapshot quota up front

import json
import logging
import openstack
import os
import subprocess
import time
from collections import namedtuple

from .hosts import OpenstackHost
from .state import STATE


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
     'size', # Volume size reported by OpenStack, in GB
     'url' # Final NBD export address from source conversion host
    ])
class OpenStackSourceHost(_BaseSourceHost):
    """ Export volumes from an OpenStack instance. """

    def __init__(self, data, agent_sock):
        self.default_timeout = 600 # Maximum wait for openstacksdk operations

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
        return subprocess.check_output(command, env=environment)

    def _converter_val(self, args):
        """ Run a command on the source conversion host and get return code. """
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
            logging.info('Waiting 300s for source VM to stop...')
            self.conn.compute.wait_for_server(server, 'SHUTOFF', wait=300)

    def _get_attachment(self, volume, vm):
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
                size=volume.size, url=None)

    def _detach_data_volumes_from_source(self):
        """
        Detach data volumes from source VM, and pretend to "detach" the boot
        volume by creating a new volume from a snapshot of the VM. Volume map
        step two: replace boot disk ID with this new volume's ID.
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
                    volume_id=volume_id, timeout=self.default_timeout)
                self.root_snapshot = root_snapshot

                # Create a new volume from the snapshot
                logging.info('Creating new volume from root snapshot')
                root_volume_copy = self.conn.create_volume(wait=True,
                        name='rhosp-migration-{}'.format(volume_id),
                        snapshot_id=root_snapshot.id,
                        size=root_snapshot.size,
                        timeout=self.default_timeout)
                self.root_volume_copy = root_volume_copy

                # Update the volume map with the new volume ID
                self.volume_map[path] = mapping._replace(
                    source_id=root_volume_copy.id)
            else: # Detach non-root volumes
                logging.info('Detaching %s from %s', volume, sourcevm.id)
                self.conn.detach_volume(server=sourcevm, volume=volume,
                    wait=True, timeout=self.default_timeout)

    def _wait_for_volume_dev_path(self, volume, vm, empty=False):
        attachment = self._get_attachment(vm, volume)

    def _attach_volumes_to_converter(self):
        """ Attach all the source volumes to the conversion host """
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s to conversion host...', volume_id)
            self.conn.attach_volume(server=self._converter(), volume=volume,
                wait=True, timeout=self.default_timeout)
            time.sleep(15) # TODO: wait until actually attached and has devpath

    def _export_volumes_from_converter(self):
        """
        SSH to source conversion host and start an NBD export. Start the UCI
        with /dev/vdb, /dev/vdc, etc. attached, then pass JSON input to request
        nbdkit exports from the V2V wrapper. Find free ports up front so they
        can be passed to the container (TODO).
        """
        logging.info('Exporting volumes from source conversion host...')

        # Expose and forward the ports we are going to tell nbdkit to use
        ssh_args = []
        nbd_ports = []
        forward_ports = ['-N', '-T']
        device_list = []
        port_map = {}
        port = 10809
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            dev_path = self._get_attachment(volume, self._converter()).device
            uci_dev_path = dev_path + '-v2v'
            logging.info('Exporting device %s...', dev_path)
            port_map[uci_dev_path] = port
            forward_ports.extend(['-L', '{0}:localhost:{0}'.format(port)])
            nbd_ports.extend(['-p', '{0}:{0}'.format(port)])
            device_list.extend(['--device', dev_path + ':' + uci_dev_path])
            self.volume_map[path] = mapping._replace(source_dev=dev_path,
                url='nbd://localhost:'+str(port))
            logging.info('Volume map so far: %s', self.volume_map)
            port += 1

        # Get SSH to forward the NBD ports to localhost
        self.forwarding_process = self._converter_sub(forward_ports)

        # Copy the port map input
        self._converter_out(['mkdir', '-p', '/v2v/input'])
        ports = json.dumps({'nbd_export_only': port_map})
        nbd_conversion = '/tmp/nbd_conversion.json'
        with open(nbd_conversion, 'w+') as conversion:
            conversion.write(ports)
        self._converter_scp(nbd_conversion, '/v2v/input/conversion.json')
        self._converter_out(['rm', '-f', '/v2v/uci.id'])

        # Run UCI on source conversion host. Create a temporary directory to
        # use as the UCI's /data directory (TODO). That way more than one can
        # run at a time.
        ssh_args.extend(['sudo', 'podman', 'run', '--detach'])
        ssh_args.extend(['-v', '/v2v:/data:z'])
        ssh_args.extend(nbd_ports)
        ssh_args.extend(device_list)
        ssh_args.extend(['localhost/v2v-updater', '/usr/local/bin/entrypoint'])
        self.uci_id = self._converter_out(ssh_args).strip()
        self.uci_id = self.uci_id.decode('utf-8')
        logging.debug('Source UCI container ID: %s', self.uci_id)

        # Make sure export worked by checking the exports
        logging.info('Waiting for NBD exports from source container...')
        while self._converter_val(['test', '-f', '/v2v/nbdready']) != 0:
            time.sleep(5)
        for disk, port in port_map.items():
            cmd = ['qemu-img', 'info', 'nbd://localhost:{}'.format(port)]
            image_info = subprocess.check_output(cmd)
            logging.info('qemu-img info for %s: %s', disk, image_info)

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
        except Exception as error:
            pass

    def _detach_volumes_from_converter(self):
        """ Detach volumes from conversion host. """
        for volume in self._converter().volumes:
            logging.info('Inspecting volume %s', volume["id"])
            v = self.conn.volume.get_volume(volume["id"])
            for attachment in v.attachments:
                if attachment["server_id"] == self._converter().id:
                    if attachment["device"] == "/dev/vda":
                        logging.info('This is the root volume for this VM')
                    else:
                        logging.info('This volume is a data disk for this VM')
                        self.conn.detach_volume(server=self._converter(),
                                wait=True, timeout=self.default_timeout,
                                volume=v)
                        logging.info('Detached.')
                else:
                    logging.info('Attachment is not part of current VM?')
        time.sleep(5)

    def _attach_data_volumes_to_source(self):
        """ Clean up the copy of the root volume and reattach data volumes. """
        for path, mapping in self.volume_map.items():
            if path == '/dev/vda':
                # Delete the copy of the source root disk
                logging.info('Removing copy of root volume')
                self.conn.delete_volume(name_or_id=self.root_volume_copy.id,
                    wait=True, timeout=self.default_timeout)
                logging.info('Deleting root device snapshot')
                self.conn.delete_volume_snapshot(timeout=self.default_timeout,
                    wait=True, name_or_id=self.root_snapshot.id)
            else:
                # Attach data volumes back to source VM
                logging.info('Re-attaching volumes to source VM...')
                volume = self.conn.get_volume_by_id(mapping.source_id)
                logging.info('Attaching %s back to source VM...', volume_id)
                self.conn.attach_volume(volume=volume, wait=True,
                    server=self._source_vm(), timeout=self.default_timeout)

    def _create_destination_volumes(self): 
        logging.info('Creating volumes on destination cloud')
        for path, mapping in self.volume_map.items():
            volume_id = mapping.source_id
            volume = self.conn.get_volume_by_id(volume_id)
            new_volume = self.dest_conn.create_volume(name=volume.name,
                bootable=volume.bootable, description=volume.description,
                size=volume.size, wait=True, timeout=self.default_timeout)
            self.volume_map[path] = mapping._replace(dest_id=new_volume.id)

    def _attach_destination_volumes(self):
        logging.info('Attaching volumes to destination wrapper')
        for path, mapping in self.volume_map.items():
            volume_id = mapping.dest_id
            volume = self.dest_conn.get_volume_by_id(volume_id)
            self.dest_conn.attach_volume(volume=volume, wait=True,
                server=self._destination(), timeout=self.default_timeout)
            logging.info('Waiting for volume to appear in destination wrapper')
            time.sleep(20)
            volume = self.dest_conn.get_volume_by_id(volume_id)
            dev_path = self._get_attachment(volume, self._destination()).device
            self.volume_map[path] = mapping._replace(dest_dev=dev_path)
        time.sleep(5)

    def _convert_destination_volumes(self):
        logging.info('Converting volumes...')
        for path, mapping in self.volume_map.items():
            logging.info('Converting source VM\'s %s: %s', path, str(mapping))
            overlay = '/tmp/'+os.path.basename(mapping.dest_dev)+'.qcow2'

            def _log_convert(source_disk, source_format):
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
        logging.info('Detaching volumes from destination wrapper.')
        for path, mapping in self.volume_map.items():
            volume_id = mapping.dest_id
            volume = self.dest_conn.get_volume_by_id(volume_id)
            self.dest_conn.detach_volume(volume=volume, wait=True,
                server=self._destination(), timeout=self.default_timeout)

    def get_disk_ids(self):
        disk_ids = {}
        for path, mapping in self.volume_map.items():
            volume_id = mapping.dest_id
            disk_ids[path] = volume_id
        return disk_ids
