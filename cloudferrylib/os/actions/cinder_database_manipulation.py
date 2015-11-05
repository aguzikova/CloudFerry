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
DEFAULT = 'default'
SRC = 'src'
DST = 'dst'
CLOUD = 'cloud'
RES = 'res'
CFG = 'cfg'


def _remote_runner(cloud):
    return remote_runner.RemoteRunner(cloud[HOST], cloud[CFG].ssh_user,
                                      cloud[CFG].ssh_sudo_password)


def _volume_types_map(data):
    return dict([(t['name'], t['id']) for t in data.get('volume_types', [])])


def _volume_types(data):
    return data.get('volume_types', [])


def _modify_data(data):
    for volume in data['volumes']:
        if volume.get('status', '') == 'in-use':
            volume['mountpoint'] = None
            volume['status'] = 'available'
            volume['instance_uuid'] = None
            volume['attach_status'] = 'detached'

    # disregard volume types
    if 'volume_types' in data:
        del data['volume_types']


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
        self.cloud = {
            SRC: {
                CLOUD: self.src_cloud,
                RES: self.src_cloud.resources.get(utils.STORAGE_RESOURCE),
                CFG: self.cfg.src,
            },
            DST: {
                CLOUD: self.dst_cloud,
                RES: self.dst_cloud.resources.get(utils.STORAGE_RESOURCE),
                CFG: self.cfg.dst,
            }
        }

        self.data = jsondate.loads(data_from_namespace)

        search_opts = kwargs.get('search_opts_tenant', {})
        self.dst_data = jsondate.loads(
            self.cloud[DST][RES].read_db_info(**search_opts))

        for i in self.cloud:
            self.cloud[i][HOST] = self.cloud[i][RES].get_endpoint_host()

        self._copy_volumes()

        _modify_data(self.data)
        self.cloud[DST][RES].deploy(jsondate.dumps(self.data))

    def _run_cmd(self, cloud, cmd):
        runner = _remote_runner(cloud)
        with settings(gateway=cloud[CLOUD].getIpSsh(),
                      connection_attempts=self.ssh_attempts):
            output = runner.run(cmd)
            res = output.split('\r\n')
            return res if len(res) > 1 else res[0]

    def _run_repeat_on_errors(self, cloud, cmd):
        runner = _remote_runner(cloud)
        with settings(gateway=cloud[CLOUD].getIpSsh(),
                      connection_attempts=self.ssh_attempts):
            runner.run_repeat_on_errors(cmd)

    def _find_dir(self, cloud, paths, v):
        if not paths:
            return None
        cmd = r'find / \( ' + \
            " -o ".join(['-path "%s/*"' % p for p in paths]) + \
            r' \) -type f -name "volume-%s"' % v['id']
        return self._run_cmd(cloud, cmd)

    def _run_rsync(self, src, dst):
        cmd = (
            'rsync --inplace -a -e "ssh -o StrictHostKeyChecking=no" '
            '%s %s@%s:%s' % (src, self.cloud[DST][CFG].ssh_user,
                             self.cloud[DST][HOST], dst)
        )
        self._run_repeat_on_errors(self.cloud[SRC], cmd)

    def _rsync(self, src, dst):
        if not isinstance(dst, list):
            dst = [dst]

        for try_dst in dst:
            try:
                self._run_rsync(src, try_dst)
                return try_dst
            except RemoteExecutionError:
                pass
        else:
            raise RemoteExecutionError(
                'Copy to {dst} failed.'.format(dst=dst))

    def _mount_output(self, cloud, vt=None):
        if vt:
            cmd = (
                "for exp in "
                "$(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ "
                "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
                "do mount | "
                "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
                "{print $3\"%s\"$1}}'; done"
            ) % (vt['name'], MOUNT_DELIM)
        else:
            # default nfs_shares_config
            cmd = (
                "for exp in "
                "$(awk -F= '/nfs_shares_config/ "
                "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
                "do mount | "
                "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
                "{print $3\"%s\"$1}}'; done"
            ) % MOUNT_DELIM
        res = self._run_cmd(cloud, cmd)
        return res if isinstance(res, list) else [res]

    def _mount_dirs(self, cloud, vt=None):
        if vt:
            cmd = (
                "for exp in "
                "$(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ "
                "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
                "do mount | "
                "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
                "{print $3}}'; done"
            ) % vt['name']
        else:
            # default nfs_shares_config
            cmd = (
                "for exp in "
                "$(awk -F= '/nfs_shares_config/ "
                "{print $2; exit}' /etc/cinder/cinder.conf | xargs cat); "
                "do mount | "
                "awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") "
                "{print $3}}'; done"
            )
        res = self._run_cmd(cloud, cmd)
        return res if isinstance(res, list) else [res]

    def _vt_map(self):
        # host volume_type_id->hostname map
        # cached property
        if not hasattr(self, 'dst_volume_types'):
            self.dst_volume_types = _volume_types_map(self.dst_data)
        res = dict(
            [(vt['id'], self.dst_volume_types[vt['name']])
             for vt in _volume_types(self.data)
             if vt['name'] in self.dst_volume_types])
        return res

    def _dst_host(self, vtid=None):
        # vtid -> dst_host
        # cached property
        if not hasattr(self, 'dst_hosts'):
            self.dst_hosts = \
                [i.host for i in
                 self.cloud[DST][RES].cinder_client.services.list(
                     binary=CINDER_VOLUME) if i.state == 'up']
        # cached property
        if not hasattr(self, 'dst_volume_types'):
            self.dst_volume_types = _volume_types_map(self.dst_data)

        host_map = {}
        for h in self.dst_hosts:
            if '@' in h:
                _, t = h.split('@')
                if t in self.dst_volume_types:
                    host_map[self.dst_volume_types[t]] = h

        host = host_map.get(vtid, self.dst_hosts[0])
        return host

    def _dst_mount_info(self):
        # cached property
        if not hasattr(self, 'dst_mount'):
            self.dst_mount = {}
            if not _volume_types(self.dst_data):
                self.dst_mount[DEFAULT] = [
                    line.split(MOUNT_DELIM)
                    for line in self._mount_output(self.cloud[DST])
                ]
            for vt in _volume_types(self.dst_data):
                self.dst_mount[vt['id']] = []
                output = self._mount_output(self.cloud[DST], vt)
                for line in output:
                    self.dst_mount[vt['id']].append(line.split(MOUNT_DELIM))

        return self.dst_mount

    def _dir_to_provider(self, dst):
        # cached property
        if not hasattr(self, 'dst_dir_to_provider'):
            mount_info = self._dst_mount_info()
            if _volume_types(self.dst_data):
                self.dst_dir_to_provider = \
                    dict([t for vt in self.dst_data['volume_types']
                          for t in mount_info[vt['id']]])
            else:
                self.dst_dir_to_provider = \
                    dict([t for t in mount_info[DEFAULT]])

        return self.dst_dir_to_provider[dst]

    def _provider_to_vtid(self, provider):
        # cached property
        if not hasattr(self, 'dst_provider_to_vtid'):
            mount_info = self._dst_mount_info()
            if _volume_types(self.dst_data):
                self.dst_provider_to_vtid = \
                    dict([(t[1], vt['id'])
                          for vt in self.dst_data['volume_types']
                          for t in mount_info[vt['id']]])
            else:
                self.dst_provider_to_vtid = \
                    dict([(t[1], None) for t in mount_info[DEFAULT]])
        return self.dst_provider_to_vtid[provider]

    def _path_map(self):
        paths = {SRC: {'all': []}, DST: {'all': []}}
        paths[SRC][BY_VTID] = {}

        if not _volume_types(self.data):
            paths[SRC][ALL] = self._mount_dirs(self.cloud[SRC])

        for vt in _volume_types(self.data):
            paths[SRC][BY_VTID][vt['id']] = \
                self._mount_dirs(self.cloud[SRC], vt)

        paths[DST][BY_VTID] = {}
        mount_info = self._dst_mount_info()

        if not _volume_types(self.dst_data):
            for t in mount_info.get(DEFAULT):
                d = t[0]
                paths[DST][ALL].append(d)

        for vt in _volume_types(self.dst_data):
            paths[DST][BY_VTID][vt['id']] = []
            for t in mount_info[vt['id']]:
                d = t[0]
                paths[DST][BY_VTID][vt['id']].append(d)

        for i in self.cloud:
            for sd in sorted(paths[i][BY_VTID].values()):
                paths[i][ALL].extend(sd)

        return paths

    def _paths(self, position, vtid=None):
        # cached property
        if not hasattr(self, 'path_map'):
            self.path_map = self._path_map()
        if vtid:
            return self.path_map[position][BY_VTID][vtid]
        return self.path_map[position][ALL]

    def _copy_volumes(self):
        vt_map = self._vt_map()

        for v in self.data['volumes']:
            volume_type_id = v.get('volume_type_id', None)
            srcpaths = self._paths(SRC, volume_type_id)

            if volume_type_id in vt_map:
                # src -> dst
                v['volume_type_id'] = vt_map.get(volume_type_id, None)
            else:
                v['volume_type_id'] = None

            dstpaths = self._paths(DST, v['volume_type_id'])

            src = self._find_dir(self.cloud[SRC], srcpaths, v)
            dst = self._find_dir(self.cloud[DST], dstpaths, v)

            if not dst:
                dst = dstpaths
            else:
                dst = '/'.join(dst.split('/')[0:-1])

            dst = self._rsync(src, dst)

            v['provider_location'] = self._dir_to_provider(dst)
            vtid = self._provider_to_vtid(v['provider_location'])
            v[HOST] = self._dst_host(vtid)
