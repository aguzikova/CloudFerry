from novaclient.v1_1 import client as novaClient
from cinderclient.v1 import client as cinderClient
#from quantumclient.v2_0 import client as quantumClient
from neutronclient.v2_0 import client as neutronClient
from glanceclient.v1 import client as glanceClient
from keystoneclient.v2_0 import client as keystoneClient





class osCommon(object):

    """

    Common class for getting openstack client objects

    """
    
    def __init__(self, config):
        self.keystone_client = self.get_keystone_client(config)
        self.nova_client = self.get_nova_client(config)
        self.cinder_client = self.get_cinder_client(config)
        self.neutron_client = self.get_neutron_client(config)
        self.glance_client = self.get_glance_client(self.keystone_client)
        
    @staticmethod
    def get_nova_client(params):

        """ Getting nova client """

        return novaClient.Client(params["user"],
                                 params["password"],
                                 params["tenant"],
                                 "http://" + params["host"] + ":35357/v2.0/")

    @staticmethod
    def get_cinder_client(params):

        """ Getting cinder client """

        return cinderClient.Client(params["user"],
                                   params["password"],
                                   params["tenant"],
                                   "http://" + params["host"] + ":35357/v2.0/")

    @staticmethod
    def get_neutron_client(params):

        """ Getting neutron(quantun) client """

        return neutronClient.Client(username=params["user"],
                                    password=params["password"],
                                    tenant_name=params["tenant"],
                                    auth_url="http://" + params["host"] + ":35357/v2.0/")

    @staticmethod
    def get_keystone_client(params):

        """ Getting keystone client """

        keystoneClientForToken = keystoneClient.Client(username=params["user"],
                                                       password=params["password"],
                                                       tenant_name=params["tenant"],
                                                       auth_url="http://" + params["host"] + ":35357/v2.0/")
        return keystoneClient.Client(token=keystoneClientForToken.auth_ref["token"]["id"],
                                     endpoint="http://" + params["host"] + ":35357/v2.0/")

    @staticmethod
    def get_glance_client(keystone_client):

        """ Getting glance client """

        endpoint_glance = osCommon.get_endpoint_by_name_service(keystone_client, 'glance')
        return glanceClient.Client(endpoint_glance, token=keystone_client.auth_token_from_user)

    @staticmethod
    def get_id_service(keystone_client, name_service):

        """ Getting service_id from keystone """

        for service in keystone_client.services.list():
            if service.name == name_service:
                return service
        return None

    @staticmethod
    def get_public_endpoint_service_by_id(keystone_client, service_id):
        for endpoint in keystone_client.endpoints.list():
            if endpoint.service_id == service_id:
                return endpoint.publicurl
        return None

    @staticmethod
    def get_endpoint_by_name_service(keystone_client, name_service):
        return osCommon.get_public_endpoint_service_by_id(keystone_client, osCommon.get_id_service(keystone_client,
                                                                                                   name_service).id)

