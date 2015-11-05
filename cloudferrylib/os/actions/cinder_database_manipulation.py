# Copyright (c) 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the License);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an AS IS BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and#
# limitations under the License.


import abc
from cloudferrylib.base.action import action
from cloudferrylib.base.exception import AbortMigrationError
from cloudferrylib.utils import remote_runner
from cloudferrylib.utils.remote_runner import RemoteExecutionError
from cloudferrylib.utils import utils
from fabric.context_managers import settings

import jsondate

NAMESPACE_CINDER_CONST = "cinder_database"

CINDER_VOLUME = "cinder-volume"
HOST = 'host'
BY_VTID = 'by_vtid'
ALL = 'all'
MOUNT_DELIM = '='


class CinderDatabaseInteraction(action.Action):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass

    def get_resource(self):
        cinder_resource = self.cloud.resources.get(
            utils.STORAGE_RESOURCE)
        if not cinder_resource:
            raise AbortMigrationError(
                "No resource {res} found".format(res=utils.STORAGE_RESOURCE))
        return cinder_resource


class GetVolumesDb(CinderDatabaseInteraction):

    def run(self, *args, **kwargs):
        search_opts = kwargs.get('search_opts_tenant', {})
        return {NAMESPACE_CINDER_CONST:
                self.get_resource().read_db_info(**search_opts)}


class WriteVolumesDb(action.Action):

    """
    Copy volumes' data on nfs backends.

    Work via rsync, can handle big files
    and resume after errors.
    Depends on 'GetVolumesDb' action, it must be run first.

    """

    def run(self, *args, **kwargs):
        data_from_namespace = kwargs.get(NAMESPACE_CINDER_CONST)
        if not data_from_namespace:
            raise AbortMigrationError(
                "Cannot read attribute {attribute} from namespace".format(
                    attribute=NAMESPACE_CINDER_CONST))

        self.ssh_attempts = self.cfg.migrate.ssh_connection_attempts
        cloud = {
            'src': {
                'cloud': self.src_cloud,
                'res': self.src_cloud.resources.get(utils.STORAGE_RESOURCE),
                'cfg': self.cfg.src,
            },
            'dst': {
                'cloud': self.dst_cloud,
                'res': self.dst_cloud.resources.get(utils.STORAGE_RESOURCE),
                'cfg': self.cfg.dst,
            }
        }

        data = jsondate.loads(data_from_namespace)

        search_opts = kwargs.get('search_opts_tenant', {})
        dst_data = jsondate.loads(
            cloud['dst']['res'].read_db_info(**search_opts))

        for i in cloud:
            cloud[i]['host'] = cloud[i]['res'].get_endpoint_host()

        self.copy_volumes(cloud, data, dst_data)

        self.deploy_data(cloud['dst'], data)

    @staticmethod
    def get_remote_runner(cloud):
        return remote_runner.RemoteRunner(cloud['host'],
                                          cloud['cfg'].ssh_user,
                                          cloud['cfg'].ssh_sudo_password)

    def run_cmd(self, cloud, cmd):
        runner = self.get_remote_runner(cloud)
        with settings(gateway=cloud['cloud'].getIpSsh(),
                      connection_attempts=self.ssh_attempts):
            output = runner.run(cmd)
            res = output.split('\r\n')
            return res if len(res) > 1 else res[0]

    def run_repeat_on_errors(self, cloud, cmd):
        runner = self.get_remote_runner(cloud)
        with settings(gateway=cloud['cloud'].getIpSsh(),
                      connection_attempts=self.ssh_attempts):
            runner.run_repeat_on_errors(cmd)

    def find_dir(self, cloud, paths, v):
        cmd = r'find / \( ' + \
            " -o ".join(['-path "%s/*"' % p for p in paths]) + \
            r' \) -type f -name "volume-%s"' % v['id']
        return self.run_cmd(cloud, cmd)

    def run_rsync(self, cloud, src, dst):
        cmd = (
            'rsync --inplace -a -e "ssh -o StrictHostKeyChecking=no" '
            '%s %s@%s:%s' % (src, cloud['dst']['cfg'].ssh_user,
                             cloud['dst']['host'], dst)
        )
        self.run_repeat_on_errors(cloud['src'], cmd)

    def rsync(self, cloud, src, dst):
        if not isinstance(dst, list):
            dst = [dst]

        for try_dst in dst:
            try:
                self.run_rsync(cloud, src, try_dst)
                return try_dst
            except RemoteExecutionError:
                pass
        else:
            raise RemoteExecutionError(
                'Copy to {dst} failed.'.format(dst=dst))

    def get_mount_output(self, cloud, vt):
        cmd = (
            "for exp in "
            "$(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ "
            "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
            "do mount | "
            "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
            "{print $3\"=\"$1}}'; done"
        ) % vt['name']
        res = self.run_cmd(cloud, cmd)
        return res if isinstance(res, list) else [res]

    def get_mount_dirs(self, cloud, vt):
        cmd = (
            "for exp in "
            "$(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ "
            "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
            "do mount | "
            "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
            "{print $3}}'; done"
        ) % vt['name']
        res = self.run_cmd(cloud, cmd)
        return res if isinstance(res, list) else [res]

    @staticmethod
    def deploy_data(cloud, data):
        for volume in data['volumes']:
            if volume['status'] == 'in-use':
                volume['mountpoint'] = None
                volume['status'] = 'available'
                volume['instance_uuid'] = None
                volume['attach_status'] = 'detached'

        # disregard volume types
        del data['volume_types']

        #import ipdb; ipdb.set_trace()
        cloud['res'].deploy(jsondate.dumps(data))

    @staticmethod
    def get_volume_types(data):
        return dict([(t['name'], t['id']) for t in data['volume_types']])

    def get_vt_map(self, cloud, data, dst_data):
        # host volume_type_id->hostname map
        # cached property
        if not hasattr(self, 'dst_volume_types'):
            self.dst_volume_types = self.get_volume_types(dst_data)
        res = dict(
            [(vt['id'], self.dst_volume_types[vt['name']])
             for vt in data['volume_types']
             if vt['name'] in self.dst_volume_types])
        return res

    def get_dst_host(self, cloud, data, vtid):
        # vtid -> dst_host
        # cached property
        if not hasattr(self, 'dst_hosts'):
            self.dst_hosts = \
                [i.host for i in
                 cloud['res'].cinder_client.services.list(
                     binary=CINDER_VOLUME) if i.state == 'up']
        # cached property
        if not hasattr(self, 'dst_volume_types'):
            self.dst_volume_types = self.get_volume_types(data)

        host_map = {}
        for h in self.dst_hosts:
            if '@' in h:
                _, t = h.split('@')
                host_map[self.dst_volume_types[t]] = h

        host = host_map.get(vtid, self.dst_hosts[0])
        return host

    def dst_mount_info(self, cloud, data):
        # cached property
        if not hasattr(self, 'dst_mount'):
            self.dst_mount = {}
            for vt in data['volume_types']:
                self.dst_mount[vt['id']] = []
                output = self.get_mount_output(cloud, vt)
                for line in output:
                    self.dst_mount[vt['id']].append(line.split(MOUNT_DELIM))

        return self.dst_mount

    def dir_to_provider(self, cloud, data):
        # cached property
        if not hasattr(self, 'dst_dir_to_provider'):
            mount_info = self.dst_mount_info(cloud, data)
            self.dst_dir_to_provider = dict([t
                                             for vt in data['volume_types']
                                             for t in mount_info[vt['id']]])
        return self.dst_dir_to_provider

    def provider_to_vtid(self, cloud, data):
        # cached property
        if not hasattr(self, 'dst_provider_to_vtid'):
            mount_info = self.dst_mount_info(cloud, data)
            self.dst_provider_to_vtid = dict([(t[1], vt['id'])
                                              for vt in data['volume_types']
                                              for t in mount_info[vt['id']]])
        return self.dst_provider_to_vtid

    def get_path_map(self, cloud, data, dst_data):
        paths = {'src': {'all': []}, 'dst': {'all': []}}
        paths['src'][BY_VTID] = {}

        for vt in data['volume_types']:
            paths['src'][BY_VTID][vt['id']] = \
                self.get_mount_dirs(cloud['src'], vt)

        paths['dst'][BY_VTID] = {}
        mount_info = self.dst_mount_info(cloud['dst'], dst_data)
        for vt in dst_data['volume_types']:
            paths['dst'][BY_VTID][vt['id']] = []
            for t in mount_info[vt['id']]:
                d = t[0]
                paths['dst'][BY_VTID][vt['id']].append(d)

        for i in cloud:
            for sd in paths[i][BY_VTID].values():
                paths[i]['all'].extend(sd)

        return paths

    def get_paths(self, cloud, data, dst_data, cloud_type, vtid):
        # cached property
        if not hasattr(self, 'path_map'):
            self.path_map = self.get_path_map(cloud, data, dst_data)
        if vtid:
            return self.path_map[cloud_type][BY_VTID][vtid]
        return self.path_map[cloud_type][ALL]

    def copy_volumes(self, cloud, data, dst_data):
        vt_map = self.get_vt_map(cloud, data, dst_data)

        for v in data['volumes']:
            srcpaths = self.get_paths(cloud, data, dst_data,
                                      'src', v['volume_type_id'])

            if v['volume_type_id'] in vt_map:
                # src -> dst
                v['volume_type_id'] = vt_map.get(v['volume_type_id'], None)
            else:
                v['volume_type_id'] = None

            dstpaths = self.get_paths(cloud, data, dst_data,
                                      'dst', v['volume_type_id'])

            src = self.find_dir(cloud['src'], srcpaths, v)
            dst = self.find_dir(cloud['dst'], dstpaths, v)

            if not dst:
                dst = dstpaths
            else:
                dst = '/'.join(dst.split('/')[0:-1])

            dst = self.rsync(cloud, src, dst)

            v['provider_location'] = self.dir_to_provider(cloud['dst'],
                                                          dst_data)[dst]
            vtid = self.provider_to_vtid(cloud['dst'],
                                         dst_data)[v['provider_location']]
            v['host'] = self.get_dst_host(cloud['dst'], dst_data, vtid)
