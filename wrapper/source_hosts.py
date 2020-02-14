""" Source hosts

Some migration sources require significant setup before they can export block
devices with nbdkit. This module holds the code used to set up nbdkit exports
on such sources.

For example, the current OpenStack export strategy is to shut down the source
VM, attach its volumes to a source conversion host, and export the volumes from
inside the conversion host via nbdkit. The OpenStackSourceHost class will take
care of the process up to this point, and some other class in the regular hosts
module will take care of copying the data from those exports to their final
migration destination.
"""

import json
import logging
import openstack
import os
import subprocess
import time


def detect_source_host(data, agent_sock):
    """ Create the right source host object based on the input data. """
    if 'osp_source_environment' in data:
        return OpenStackSourceHost(data, agent_sock)
    return None

class _BaseSourceHost(object):
    """ Interface for source hosts, currently just prepare and close. """

    # Interface

    def prepare_exports(self):
        """ Creates the nbdkit exports from the migration source. """
        logging.info('No preparation needed for this migration source.')

    def close_exports(self):
        """ Stops the nbdkit exports on the migration source. """
        logging.info('No cleanup needed for this migration source.')


class OpenStackSourceHost(_BaseSourceHost):
    """ Export volumes from an OpenStack instance. """

    def __init__(self, data, agent_sock):
        osp_env = data['osp_source_environment']
        osp_arg_list = ["auth_url", "username", "password",
                        "project_name", "project_domain_name",
                        "user_domain_name", "verify"]
        osp_args = {arg: osp_env[arg] for arg in osp_arg_list}
        self.source_converter = osp_env['conversion_vm_id']
        self.source_instance = osp_env['vm_id']
        self.conn = openstack.connect(**osp_args)
        self.agent_sock = agent_sock
        self.exported = False
        openstack.enable_logging()

    def prepare_exports(self):
        """ Attach the source VM's volumes to the source conversion host. """
        self._test_ssh_connection()
        self._shutdown_source_vm()
        self.root_volume_id, self.data_volume_ids = \
                self._get_root_and_data_volumes()
        self.root_volume_copy, self.root_snapshot = \
                self._detach_data_volumes_from_source()
        self._attach_volumes_to_converter()
        self._export_volumes_from_converter()
        self.exported = True

    def close_exports(self):
        """ Put the source VM's volumes back where they were. """
        if self.exported:
            self._test_ssh_connection()
            self._converter_close_exports()
            self._detach_volumes_from_converter()
            self._attach_data_volumes_to_source()
            self.exported = False

    def _source_vm(self):
        """
        Changes to the VM returned by get_server_by_id are not necessarily
        reflected in existing objects, so just get a new one every time.
        """
        return self.conn.get_server_by_id(self.source_instance)

    def _converter(self):
        """ Same idea as _source_vm. """
        return self.conn.get_server_by_id(self.source_converter)

    def _converter_out(self, args):
        """ Run a command on the source conversion host and get the output. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        command = [
            'ssh',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
            'cloud-user@'+self._converter().accessIPv4
            ]
        command.extend(args)
        return subprocess.check_output(command, env=environment)

    def _converter_val(self, args):
        """ Run a command on the source conversion host and get return code. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        command = [
            'ssh',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
            'cloud-user@'+self._converter().accessIPv4
            ]
        command.extend(args)
        return subprocess.call(command, env=environment)

    def _converter_scp(self, source, dest):
        """ Copy a file to the source conversion host. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        command = [
            'scp',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
            source,
            'cloud-user@'+self._converter().accessIPv4+':'+dest
            ]
        return subprocess.check_output(command, env=environment)

    def _converter_sub(self, args):
        """ Run a long-running command on the source conversion host. """
        environment = os.environ.copy()
        environment['SSH_AUTH_SOCK'] = self.agent_sock
        command = [
            'ssh',
            '-o', 'BatchMode=yes',
            '-o', 'StrictHostKeyChecking=no', 
            '-o', 'ConnectTimeout=10',
            'cloud-user@'+self._converter().accessIPv4
            ]
        command.extend(args)
        return subprocess.Popen(command, env=environment)

    def _test_ssh_connection(self):
        """ Quick SSH connectivity check. """
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

    def _detach_data_volumes_from_source(self):
        """
        Detach data volumes from source VM, and pretend to "detach" the boot
        volume by creating a new volume from a snapshot of the VM.
        """
        sourcevm = self._source_vm()

        # Detach non-root volumes
        for volume in self.data_volume_ids:
            v = self.conn.get_volume_by_id(volume)
            logging.info('Detaching %s from %s', volume, sourcevm.id)
            self.conn.detach_volume(server=sourcevm, volume=v, wait=True)

        # Create a snapshot of the root volume
        logging.info('Creating root device snapshot')
        root_snapshot = self.conn.create_volume_snapshot(force=True, wait=True,
                name='rhosp-migration-{}'.format(self.root_volume_id),
                volume_id=self.root_volume_id)

        # Create a new volume from the snapshot
        logging.info('Creating new volume from root snapshot')
        root_volume_copy = self.conn.create_volume(wait=True,
                name='rhosp-migration-{}'.format(self.root_volume_id),
                snapshot_id=root_snapshot.id,
                size=root_snapshot.size)
        return root_volume_copy, root_snapshot

    def _get_root_and_data_volumes(self):
        """ Get the IDs of the boot and data volumes on the source VM. """
        root_volume_id = None
        data_volume_ids = []
        for volume in self._source_vm().volumes:
            logging.info('Inspecting volume: %s', volume['id'])
            v = self.conn.volume.get_volume(volume['id'])
            for attachment in v.attachments:
                if attachment['server_id'] == self._source_vm().id:
                    if attachment['device'] == '/dev/vda':
                        logging.info('Assuming this volume is the root disk')
                        root_volume_id = attachment['volume_id']
                    else:
                        logging.info('Assuming this is a data volume')
                        data_volume_ids.append(attachment['volume_id'])
                else:
                    logging.info('Attachment is not part of current VM?')
        return root_volume_id, data_volume_ids

    def _attach_volumes_to_converter(self):
        """ Attach all the source volumes to the conversion host """
        conversion_volume_ids = [self.root_volume_copy.id]+self.data_volume_ids
        for volume_id in conversion_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s to conversion host...', volume_id)
            self.conn.attach_volume(server=self._converter(), volume=volume)
            time.sleep(15) # TODO: wait until actually attached

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
        for volume_id in [self.root_volume_copy.id] + self.data_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            dev_path = volume.attachments[0].device
            uci_dev_path = dev_path + '-v2v'
            logging.info('Exporting device %s...', dev_path)
            port_map[uci_dev_path] = port
            forward_ports.extend(['-L', '{0}:localhost:{0}'.format(port)])
            nbd_ports.extend(['-p', '{0}:{0}'.format(port)])
            device_list.extend(['--device', dev_path + ':' + uci_dev_path])
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
        ssh_args.extend(['6c7527c9f744', '/usr/local/bin/entrypoint'])
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
            if self.forwarding_process:
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
                                volume=v, wait=True)
                        logging.info('Detached.')
                else:
                    logging.info('Attachment is not part of current VM?')

    def _attach_data_volumes_to_source(self):
        """ Clean up the copy of the root volume and reattach data volumes. """

        # Delete the copy of the source root disk
        logging.info('Removing copy of root volume')
        self.conn.delete_volume(name_or_id=self.root_volume_copy.id, wait=True)
        logging.info('Deleting root device snapshot')
        self.conn.delete_volume_snapshot(name_or_id=self.root_snapshot.id,
                wait=True)

        # Attach data volumes back to source VM
        logging.info('Re-attaching volumes to source VM...')
        for volume_id in self.data_volume_ids:
            volume = self.conn.get_volume_by_id(volume_id)
            logging.info('Attaching %s back to source VM...', volume_id)
            self.conn.attach_volume(volume=volume, wait=True,
                    server=self._source_vm())
