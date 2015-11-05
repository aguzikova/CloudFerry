# Copyright 2015: Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import mock
from mock.mock import call, ANY

from tests import test
from cloudferrylib.os.actions import cinder_database_manipulation
from cloudferrylib.utils import utils

import jsondate


class WriteVolumesDbTest(test.TestCase):
    def test_run_no_volume_types(self):
        volumes = {
            "volumes": [
                {"id": "vol-1"}
            ]
        }
        fake_dst_data = {
            "volumes": [],
        }
        action, args = self._action(volumes, fake_dst_data)
        action.run(**args)

        calls = [
            call(ANY, (
                'rsync --inplace -a -e '
                '"ssh -o StrictHostKeyChecking=no" '
                '/var/lib/cinder/80a8c674d115b2a3c20f1e959bd1f20f/volume-vol-1'
                ' dst_user@dst_cinder:/var/lib/cinder/dstdir0a'
            )),
        ]
        action._run_repeat_on_errors.assert_has_calls(calls)
        expected = {
            "volumes": [
                {
                    "volume_type_id": None,
                    "host": "dst_cinder",
                    "id": "vol-1",
                    "provider_location": "/var/exports/dst0a"
                }
            ]
        }
        self.assertEqual(action.data, expected)

    def test_run_with_volume_types(self):
        fake_src_data = {
            "volumes": [
                {
                    "id": "vol-nfs1",
                    "volume_type_id": "nfs1_id",
                },
                {
                    "id": "vol-nfs2",
                    "volume_type_id": "nfs2_id",
                },
                {
                    "id": "vol-nfs3",
                    "volume_type_id": "nfs3_id",
                },
                {
                    "id": "vol",
                },
            ],
            "volume_types": [
                {
                    "id": "nfs1_id",
                    "name": "nfs1",
                },
                {
                    "id": "nfs2_id",
                    "name": "nfs2",
                },
                {
                    "id": "nfs3_id",
                    "name": "nfs3",
                },
            ]
        }
        fake_dst_data = {
            "volumes": [],
            "volume_types": [
                {
                    "id": "nfs1_dst_id",
                    "name": "nfs1",
                },
                {
                    "id": "nfs_other_id",
                    "name": "nfs_other",
                },
                {
                    "id": "nfs3_dst_id",
                    "name": "nfs3",
                },
            ]
        }

        action, args = self._action(fake_src_data, fake_dst_data)
        action.run(**args)

        calls = [
            call(ANY, ('rsync --inplace -a -e '
                       '"ssh -o StrictHostKeyChecking=no" '
                       '/var/lib/cinder/dir1a/volume-vol-nfs1 '
                       'dst_user@dst_cinder:/var/lib/cinder/dstdir1a')
                 ),
            call(ANY, ('rsync --inplace -a -e '
                       '"ssh -o StrictHostKeyChecking=no" '
                       '/var/lib/cinder/dir2a/volume-vol-nfs2 '
                       'dst_user@dst_cinder:/var/lib/cinder/dstdir1a')
                 ),
            call(ANY, ('rsync --inplace -a -e '
                       '"ssh -o StrictHostKeyChecking=no" '
                       '/var/lib/cinder/dir3a/volume-vol-nfs3 '
                       'dst_user@dst_cinder:/var/lib/cinder/dstdir3a')
                 ),
            call(ANY, ('rsync --inplace -a -e '
                       '"ssh -o StrictHostKeyChecking=no" '
                       '/var/lib/cinder/dir1a/volume-vol '
                       'dst_user@dst_cinder:/var/lib/cinder/dstdir1a')
                 ),
        ]
        action._run_repeat_on_errors.assert_has_calls(calls)

        expected = {
            "volumes": [
                {"volume_type_id": "nfs1_dst_id",
                 "host": "dst_cinder@nfs1",
                 "id": "vol-nfs1",
                 "provider_location": "/var/exports/dst1a"},
                {"volume_type_id": None,
                 "host": "dst_cinder@nfs1",
                 "id": "vol-nfs2",
                 "provider_location": "/var/exports/dst1a"},
                {"volume_type_id": "nfs3_dst_id",
                 "host": "dst_cinder@nfs3",
                 "id": "vol-nfs3",
                 "provider_location": "/var/exports/dst3a"},
                {"volume_type_id": None,
                 "host": "dst_cinder@nfs1",
                 "id": "vol",
                 "provider_location": "/var/exports/dst1a"
                 }
            ]
        }
        self.assertEqual(action.data, expected)

    def test_run_no_src_volume_types(self):
        fake_src_data = {
            "volumes": [
                {
                    "id": "vol",
                },
            ],
        }
        fake_dst_data = {
            "volumes": [],
            "volume_types": [
                {
                    "id": "nfs1_dst_id",
                    "name": "nfs1",
                },
                {
                    "id": "nfs_other_id",
                    "name": "nfs_other",
                },
                {
                    "id": "nfs3_dst_id",
                    "name": "nfs3",
                },
            ]
        }

        action, args = self._action(fake_src_data, fake_dst_data)
        action.run(**args)

        calls = [
            call(ANY, ('rsync --inplace -a -e '
                       '"ssh -o StrictHostKeyChecking=no" '
                       '/var/lib/cinder/80a8c674d115b2a3c20f1e959bd1f20f/'
                       'volume-vol '
                       'dst_user@dst_cinder:/var/lib/cinder/dstdir1a')
                 ),
        ]
        action._run_repeat_on_errors.assert_has_calls(calls)

        expected = {
            "volumes": [
                {"volume_type_id": None,
                 "host": "dst_cinder@nfs1",
                 "id": "vol",
                 "provider_location": "/var/exports/dst1a"
                 }
            ]
        }
        self.assertEqual(action.data, expected)

    def _action(self, fake_src_data, fake_dst_data):
        src_cinder_host = "src_cinder"
        dst_cinder_host = "dst_cinder"
        fake_config = utils.ext_dict(
            migrate=utils.ext_dict({'ssh_connection_attempts': 3}),
            src=utils.ext_dict({'ssh_user': 'src_user',
                                'ssh_sudo_password': 'src_passwd',
                                'ssh_host': 'src_ssh_host'}),
            dst=utils.ext_dict({'ssh_user': 'dst_user',
                                'ssh_sudo_password': 'dst_passwd',
                                'ssh_host': 'dst_ssh_host'}),
        )

        fake_src_cloud = mock.Mock()
        fake_src_storage = mock.Mock()
        fake_src_storage.get_endpoint_host = \
            mock.Mock(return_value=src_cinder_host)
        fake_src_cloud.resources = {'storage': fake_src_storage}

        fake_dst_cloud = mock.Mock()
        fake_dst_storage = mock.Mock()
        fake_dst_storage.get_endpoint_host = \
            mock.Mock(return_value=dst_cinder_host)
        fake_dst_storage.read_db_info = \
            mock.Mock(return_value=jsondate.dumps(fake_dst_data))
        fake_dst_cloud.resources = {'storage': fake_dst_storage}

        fake_init = {
            'src_cloud': fake_src_cloud,
            'dst_cloud': fake_dst_cloud,
            'cfg': fake_config
        }

        action = cinder_database_manipulation.WriteVolumesDb(fake_init)

        def mount_dirs(cloud, vt=None):
            if vt:
                vt_map = {
                    'nfs1': [
                        '/var/lib/cinder/dir1a',
                        '/var/lib/cinder/dir1b',
                    ],
                    'nfs2': [
                        '/var/lib/cinder/dir2a',
                        '/var/lib/cinder/dir2b',
                    ],
                    'nfs3': [
                        '/var/lib/cinder/dir3a',
                    ],
                }
                return vt_map[vt['name']]
            return ['/var/lib/cinder/80a8c674d115b2a3c20f1e959bd1f20f']

        action._mount_dirs = mock.MagicMock(side_effect=mount_dirs)

        action.dst_mount = {
            'nfs1_dst_id': [
                ['/var/lib/cinder/dstdir1a', '/var/exports/dst1a'],
                ['/var/lib/cinder/dstdir1b', '/var/exports/dst1b'],
            ],
            'nfs_other_id': [
                ['/var/lib/cinder/dstdir2a', '/var/exports/dst2a'],
                ['/var/lib/cinder/dstdir2b', '/var/exports/dst2b'],
            ],
            'nfs3_dst_id': [
                ['/var/lib/cinder/dstdir3a', '/var/exports/dst3a'],
            ],
        }
        if 'volume_types' not in fake_dst_data:
            action.dst_mount['default'] = [
                ['/var/lib/cinder/dstdir0a', '/var/exports/dst0a'],
                ['/var/lib/cinder/dstdir0b', '/var/exports/dst0b'],
            ]

        fake_dst_paths = [
            line[0] for t in action.dst_mount for line in action.dst_mount[t]
        ]

        def find_dir(cloud, paths, v):
            if not paths:
                return None
            p = paths[0]

            # TODO: existed
            if p in fake_dst_paths:
                return None
            return '%s/volume-%s' % (p, v['id'])

        action._find_dir = mock.MagicMock(side_effect=find_dir)

        action.dst_hosts = [
            'dst_cinder',
            'dst_cinder@nfs1',
            'dst_cinder@nfs2',
            'dst_cinder@nfs3',
        ]
        action._run_repeat_on_errors = mock.Mock()

        args = {
            cinder_database_manipulation.NAMESPACE_CINDER_CONST:
            jsondate.dumps(fake_src_data)
        }
        return action, args
