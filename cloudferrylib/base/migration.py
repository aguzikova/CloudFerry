# Copyright (c) 2015 Mirantis Inc.
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
"""Migration of objects from SRC to DST clouds."""


from cloudferrylib.utils import utils


CLOUD = 'cloud'
SRC, DST = 'src', 'dst'


class Migration(object):

    """ Map SRC objects to corresponding DST objects they migrated to."""

    def __init__(self, migration_info, resource, resource_type):
        self.migration = migration_info
        self.obj_map = None
        self.resource = resource
        self.resource_type = resource_type
        # get singular from plural resource_type removing 's' in the end
        self.obj_name = resource_type[:-1]

    def map_migrated_objects(self):
        """Build map SRC -> DST object IDs.

        :return: dict

        """

        for attr in ('resource', 'resource_type'):
            if not hasattr(self, attr):
                raise NotImplementedError()

        objs = {
            pos: self.read_objects(pos, name=self.resource_type)
            for pos in (SRC, DST)
        }

        obj_map = dict(
            [(src[self.obj_name]['id'], dst[self.obj_name]['id'])
             for src in objs[SRC] for dst in objs[DST]
             if self.obj_identical(src[self.obj_name],
                                   dst[self.obj_name])])
        return obj_map

    def migrated_object_id(self, src_object_id):
        """ Get migrated object ID by SRC object ID.

        :return: DST object ID

        """
        if self.obj_map is None:
            self.obj_map = self.map_migrated_objects()
        return self.obj_map.get(src_object_id, None)

    def identical(self, src_id, dst_id):
        """ Check if SRC object with `src_id` === DST object with `dst_id`.

        :return: boolean

        """
        return dst_id == self.migrated_object_id(src_id)

    def obj_identical(self, src, dst):
        """ Check if SRC object `src` === DST object `dst`.

        :return: boolean

        """
        raise NotImplementedError()

    def read_objects(self, pos, name=None):
        """Read objects info from `pos` cloud.

        :return: list

        """
        res = self.migration[pos][CLOUD].resources.get(self.resource)
        objs = res.read_info()[name if name else self.resource]
        return objs.values() if isinstance(objs, dict) else objs


class ImageMigrationMap(Migration):

    """Migration of glance images from SRC to DST clouds."""

    def __init__(self, migration_info, resource_type=utils.IMAGES_TYPE):
        self.tn_map = IdentityMigrationMap(migration_info)

        Migration.__init__(self, migration_info, resource=utils.IMAGE_RESOURCE,
                           resource_type=resource_type)

    def obj_identical(self, src, dst):
        """Compare src and dst objects from resource info.

        :return: boolean

        """

        ignore_fields = ['id', 'resource', 'owner']

        for field in src:
            if field not in ignore_fields and \
                    src[field] != dst[field]:
                return False
            if field == 'owner' and \
                    not self.tn_map.identical(src[field], dst[field]):
                return False
        return True


class IdentityMigrationMap(Migration):

    """Migration of `resource_type` identities from SRC to DST clouds."""

    def __init__(self, migration_info, resource_type=utils.TENANTS_TYPE):
        Migration.__init__(self, migration_info,
                           resource=utils.IDENTITY_RESOURCE,
                           resource_type=resource_type)

    def obj_identical(self, src, dst):
        """Compare src and dst objects from resource info.

        :return: boolean

        """
        ignore_fields = ['id', 'description']

        for field in src:
            if field not in ignore_fields and \
                    src[field] != dst[field]:
                return False
        return True
