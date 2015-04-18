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
            'api_dataset_home': 'api/datasets/',
            'api_get_cdts': 'api/datasets/get-datatypes/',
            'api_get_dataset': 'api/datasets/get-datasets/',
            'api_get_dataset_page': 'api/datasets/get-datasets/',  # page
            'api_dataset_add': 'api/datasets/add-dataset/',
            'api_pipelines_home': 'api/pipelines/',
            'api_pipelines_get': 'api/pipelines/get-pipelines/',
#            'api_pipelines_get_page': 'api/pipelines/get-pipelines/(?P<page>\d+)',
            'api_pipelines_startrun': 'api/pipelines/start-run/',
            'api_pipelines_get_the_runs': 'api/pipelines/get-active-runs/',
#            'api_pipelines_runstat': 'api/pipelines/run-status/(?P<rtp_id>\d+)',
        }

    @staticmethod
    def get_token(url, username, password):
        response = requests.post(url + "api/token-auth/", {'username': username, 'password': password})

        try:
            dict = response.json()
            return dict['token']
        except (ValueError, KeyError):
            return None

    def _request(self, endpoint, method='GET', data={}, arg='', files={}):
        """
        Internal

        :param endpoint:
        :return:
        """

        # If we have a quick @ tag, lookup the
        if endpoint[0] == '@':
            endpoint = self.endpoint_map[endpoint[1:]] + str(arg)

        headers = {'Authorization': 'Token %s' % self.token}

        # Choose method
        if method.upper() == 'GET':
            response = requests.get(self.server_url + endpoint, headers=headers)
        else:
            response = requests.post(self.server_url  + endpoint, data, headers=headers, files=files)
        try:
            return response.json()
        except ValueError:
            print response.content
        return None

    def get_datasets(self):
        """

        :return:
        """
        data = self._request('@api_get_dataset')
        datasets = data['datasets']

        def _load_more(next, page):
            if next is not None:
                more = self._request('@api_get_dataset_page', 'GET', {}, page + 1)
                datasets += more['datasets']
                _load_more(more['next'], page + 1)
        _load_more(data['next'], 1)

        return [Dataset(d) for d in datasets]

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
        data = self._request('@api_pipelines_get')
        return [PipelineFamily(p) for p in data['families']]

    def get_cdts(self):
        """

        :return:
        """
        pass

    def add_dataset(self, name, description, handle, cdt=None):
        """

        :return:
        """
        data = self._request('@api_dataset_add', 'POST', {
            'name': name,
            'description': description,
            'compound_datatype': cdt.id if cdt is not None else '__raw__'
        }, '', {
            'dataset_file': handle,
        })
        return Dataset(data)

    def run_pipeline(self, pipeline, inputs):
        """
        Checks that a pipeline has the correct inputs, then
        submits the job to kive.

        :param pipeline: A Pipeline Object
        :param inputs: A list of Datasets
        :return: A RunStatus object
        """
        pass

