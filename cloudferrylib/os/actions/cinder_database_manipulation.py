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
from cloudferrylib.utils import utils
from fabric.context_managers import settings

import jsondate

NAMESPACE_CINDER_CONST = "cinder_database"


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


class WriteVolumesDb(CinderDatabaseInteraction):

    def run(self, *args, **kwargs):
        data_from_namespace = kwargs.get(NAMESPACE_CINDER_CONST)
        if not data_from_namespace:
            raise AbortMigrationError(
                "Cannot read attribute {attribute} from namespace".format(
                    attribute=NAMESPACE_CINDER_CONST))
        data = jsondate.loads(data_from_namespace)

        #dst_res = self.dst_cloud.resources.get(utils.STORAGE_RESOURCE)

        #search_opts = kwargs.get('search_opts_tenant', {})
        #dst_data = jsondate.loads(dst_res.read_db_info(**search_opts))

        ## volume_type map
        #dst_volume_types = \
        #    dict([(t['name'], t['id']) for t in dst_data['volume_types']])

        #volume_type_map = {}
        #for vt in data['volume_types']:
        #    if vt['name'] in dst_volume_types:
        #        volume_type_map[vt['id']] = dst_volume_types[vt['name']]

        ## mark attached volumes as available
        #for volume in data['volumes']:
        #    if volume['status'] == 'in-use':
        #        volume['mountpoint'] = None
        #        volume['status'] = 'available'
        #        volume['instance_uuid'] = None
        #        volume['attach_status'] = 'detached'

        #    # modify host
        #    volume['host'] = 'icehouse@nfs1'

        #    # modify volume_type_id
        #    print 'volume_type_id:', volume['volume_type_id'], ' -> '
        #    volume['volume_type_id'] = volume_type_map.get(
        #        volume['volume_type_id'], None)
        #    print ' -> ', 'volume_type_id:', volume['volume_type_id']

        #    # modify provider_location
        #    volume['provider_location'] = \
        #        '192.168.1.10:/var/exports/icehouse1'

        ## disregard volume types
        #del data['volume_types']

        #self.get_resource().deploy(jsondate.dumps(data))
        ##import ipdb; ipdb.set_trace()
        #print


class CopyVolumesData(action.Action):

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
        data = jsondate.loads(data_from_namespace)

        src_res = self.src_cloud.resources.get(utils.STORAGE_RESOURCE)
        dst_res = self.dst_cloud.resources.get(utils.STORAGE_RESOURCE)

        search_opts = kwargs.get('search_opts_tenant', {})
        dst_data = jsondate.loads(dst_res.read_db_info(**search_opts))

        src_host = src_res.get_endpoint_host()
        dst_host = dst_res.get_endpoint_host()

        def get_nfs_shares(host, gateway, cfg, ssh_attempts, cmd):
            runner = remote_runner.RemoteRunner(host, cfg.ssh_user, password=cfg.ssh_sudo_password)
            with settings(gateway=gateway, connection_attempts=ssh_attempts):
                return runner.run(cmd)

        def run_repeat_on_errors(host, gateway, cfg, ssh_attempts, cmd):
            runner = remote_runner.RemoteRunner(host, cfg.ssh_user, password=cfg.ssh_sudo_password)
            with settings(gateway=gateway, connection_attempts=ssh_attempts):
                runner.run_repeat_on_errors(cmd)


        ssh_attempts = self.cfg.migrate.ssh_connection_attempts

        cmd = "mount | awk '{if (match($1, \"^192.168.1.10:/var/exports/\") && $3 ~ \"cinder\") {print $3}}'"
        src_dirs = get_nfs_shares(src_host, self.src_cloud.getIpSsh(), self.cfg.src, ssh_attempts, cmd).split('\r\n')
        print 'src_dirs: ', src_dirs

        srcdirs = {}
        dstdirs = {}
        srcexps = {}
        dstexps = {}

        for vt in data['volume_types']:
            cmd = "cat $(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ {print $2; exit}' /etc/cinder/cinder.conf)" % vt['name']
            srcexps[vt['name']] = get_nfs_shares(src_host, self.src_cloud.getIpSsh(), self.cfg.src, ssh_attempts, cmd).split('\r\n')
            cmd = "for exp in $(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ {print $2; exit}' /etc/cinder/cinder.conf | xargs cat); do mount | awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") {print $3}}'; done" % vt['name']
            srcdirs[vt['name']] = get_nfs_shares(src_host, self.src_cloud.getIpSsh(), self.cfg.src, ssh_attempts, cmd).split('\r\n')

        for vt in dst_data['volume_types']:
            cmd = "cat $(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ {print $2; exit}' /etc/cinder/cinder.conf)" % vt['name']
            dstexps[vt['name']] = get_nfs_shares(dst_host, self.dst_cloud.getIpSsh(), self.cfg.dst, ssh_attempts, cmd).split('\r\n')
            cmd = "for exp in $(awk -F= '/\[%s\]/{i=1} /!/ {i=0} i && /nfs_shares_config/ {print $2; exit}' /etc/cinder/cinder.conf | xargs cat); do mount | awk '{if (match($1, \"^'$exp'$\") && $3 ~ \"cinder\") {print $3}}'; done" % vt['name']
            dstdirs[vt['name']] = get_nfs_shares(dst_host, self.dst_cloud.getIpSsh(), self.cfg.dst, ssh_attempts, cmd).split('\r\n')
        #cmd = "awk '{if (match($1, \"^192.168.1.10:/var/exports/\") && $3 ~ \"cinder\") {print $3}}' /etc/cinder/cinder.conf"

        #dst_dirs = get_nfs_shares(dst_host, self.dst_cloud.getIpSsh(), self.cfg.dst, ssh_attempts, cmd)

        dst_ssh_user = self.cfg.dst.ssh_user
        gateway = self.src_cloud.getIpSsh()

        # volume_type map
        dst_volume_types = \
            dict([(t['name'], t['id']) for t in dst_data['volume_types']])

        volume_type_map = {}
        for vt in data['volume_types']:
            if vt['name'] in dst_volume_types:
                volume_type_map[vt['id']] = dst_volume_types[vt['name']]

        for v in data['volumes']:
            print 'ID:', v['id']
            if v['volume_type_id']:
                volume_type_name = [vt['name'] for vt in data['volume_types'] if vt['id'] == v['volume_type_id']][0]
                srcfind = srcdirs[volume_type_name]
            else:
                srcfind = []
                for sd in srcdirs.values():
                    srcfind.extend(sd)

            if v['volume_type_id'] in volume_type_map:
                volume_type_name = [vt['name'] for vt in data['volume_types'] if vt['id'] == v['volume_type_id']][0]
                dstfind = dstdirs[volume_type_name]

                # TODO !!
                v['host'] = 'icehouse@nfs1'
                v['provider_location'] = '192.168.1.10:/var/exports/icehouse1'

                print 'volume_type_id:', v['volume_type_id'], ' -> '

                v['volume_type_id'] = volume_type_map.get(v['volume_type_id'],
                                                          None)

                print ' -> ', 'volume_type_id:', v['volume_type_id']
            else:
                dstfind = []
                for sd in dstdirs.values():
                    dstfind.extend(sd)
                v['volume_type_id'] = None

            # find srcdir
            cmd = 'find / \( ' + " -o ".join(['-path "%s/*"' % d for d in srcfind]) + ' \) -type f -name "volume-%s"' % v['id']
            print cmd
            srcdir = get_nfs_shares(src_host, self.src_cloud.getIpSsh(), self.cfg.src, ssh_attempts, cmd)
            print 'srcdir=', srcdir
            # find dstdir - if not -- create volume via api and find again
            cmd = 'find / \( ' + " -o ".join(['-path "%s/*"' % d for d in dstfind]) + ' \) -type f -name "volume-%s"' % v['id']
            print cmd
            dstdir = get_nfs_shares(dst_host, self.dst_cloud.getIpSsh(), self.cfg.dst, ssh_attempts, cmd)
            print 'dstdir=', dstdir

            if not dstdir:
                #dstdir = '%s/volume-%s' % (dstfind[0], v['id'])
                dstdir = dstfind[0]
            else:
                dstdir = '/'.join(dstdir.split('/')[0:-1])

            # TODO dstdir -> host, provider_location

            v['host'] = 'icehouse@nfs1'
            v['provider_location'] = '192.168.1.10:/var/exports/icehouse1'

            cmd = ('rsync --inplace -a -e "ssh -o StrictHostKeyChecking=no" '
                   '%s %s@%s:%s' % (srcdir, dst_ssh_user, dst_host, dstdir)
            )
            print cmd
            run_repeat_on_errors(src_host, gateway, self.cfg.src, ssh_attempts, cmd)

            # dstdir -> provider_location??
            # dstdir -> host??

        print

        #for volume in data['volumes']:
        #    # check volume_type
        #    print volume
        # mark attached volumes as available

        # Write DB
        for volume in data['volumes']:
            if volume['status'] == 'in-use':
                volume['mountpoint'] = None
                volume['status'] = 'available'
                volume['instance_uuid'] = None
                volume['attach_status'] = 'detached'

        # disregard volume types
        del data['volume_types']

        #import ipdb; ipdb.set_trace()

        dst_res.deploy(jsondate.dumps(data))
        #import ipdb; ipdb.set_trace()
        print
