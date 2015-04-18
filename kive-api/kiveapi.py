"""

"""
from .dataset import Dataset
from .pipeline import PipelineFamily
import requests


class KiveAPI(object):
    """

    """
    OAUTH_TOKEN = ""

    def __init__(self, server, token=OAUTH_TOKEN, authentication='OAuth'):
        self.server_url = server
        self.authentication_type = authentication
        self.token = token

        self.endpoint_map = {
            '',
        }

    @staticmethod
    def get_token(username, password):
        pass

    def _request(self, endpoint, method='GET', data={}):
        """
        Internal

        :param endpoint:
        :return:
        """

        # If we have a quick @ tag, lookup the
        if endpoint[0] == '@':
            endpoint = self.endpoint_map[endpoint[1:]]

        headers = {'Authorization': 'Token %s' % self.token}

        # Choose method
        if method.upper() == 'GET':
            response = requests.get(self.server_url + "/" + endpoint, headers=headers)
        else:
            response = requests.post(self.server_url + "/" + endpoint, data, headers=headers)
        try:
            return response.json()
        except ValueError:
            pass
        return None

    def get_datasets(self):
        """

        :return:
        """
        data = self._request('@api_get_dataset')
        return [Dataset(d) for d in data['datasets']]


    def get_dataset(self, dataset_id):
        """

        :param dataset_id:
        :return:
        """
        pass

    def get_pipeline_families(self):
        """

        :return:
        """
        data = self._request('@api_get_pipelines')
        return [PipelineFamily(p) for p in data['families']]

    def get_cdts(self):
        """

        :return:
        """
        pass

    def add_dataset(self):
        """

        :return:
        """
        pass

    def run_pipeline(self, pipeline, inputs):
        """
        Checks that a pipeline has the correct inputs, then
        submits the job to kive.

        :param pipeline: A Pipeline Object
        :param inputs: A list of Datasets
        :return: A RunStatus object
        """
        pass

