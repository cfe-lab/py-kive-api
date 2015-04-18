"""

"""
from .dataset import Dataset
from .pipeline import PipelineFamily
from .datatype import CompoundDatatype
from .runstatus import RunStatus

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
            'api_pipelines_get_page': 'api/pipelines/get-pipelines/',
            'api_pipelines_startrun': 'api/pipelines/start-run/',
            'api_pipelines_get_the_runs': 'api/pipelines/get-active-runs/',
            # 'api_pipelines_runstat': 'api/pipelines/run-status/(?P<rtp_id>\d+)',
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
        Internal functions used to form common requests
        to endpoints

        :param endpoint: Endpoint url
        :return: json dictionary of results
        """

        # If we have a quick @ tag, lookup the
        if endpoint[0] == '@':
            endpoint = self.endpoint_map[endpoint[1:]] + str(arg)

        headers = {'Authorization': 'Token %s' % self.token}

        # Choose method
        if method.upper() == 'GET':
            response = requests.get(self.server_url + endpoint, headers=headers)
        else:
            response = requests.post(self.server_url + endpoint, data, headers=headers, files=files)
        try:
            return response.json()
        except ValueError:
            print response.content
        return None

    def get_datasets(self):
        """
        Returns a lit

        :return:
        """
        data = self._request('@api_get_dataset')
        datasets = {'result': data['datasets']}

        def _load_more(next, dats):
            if next is not None:
                more = self._request(next[1:])
                dats['result'] += more['datasets']
                _load_more(more['next'], dats)

        _load_more(data['next'], datasets)

        return [Dataset(d) for d in datasets['result']]

    def get_dataset(self, dataset_id):
        """

        :param dataset_id:
        :return:
        """
        pass

    def get_pipeline_families(self):
        """
        Returns a list of all pipeline families and
        the pipeline revisions underneath each.

        :return: List of PipelineFamily objects
        """

        data = self._request('@api_pipelines_get')
        families = {'result': data['families']}

        def _load_more(next, fams):
            if next is not None:
                more = self._request(next[1:])
                fams['result'] += more['families']
                _load_more(more['next_page'], fams)
        _load_more(data['next_page'], families)

        return [PipelineFamily(c) for c in families['result']]

    def get_cdts(self):
        """
        Returns a list of all current compound datatypes

        :return: A list of CompoundDatatypes
        """
        data = self._request('@api_get_cdts')
        return [CompoundDatatype(c) for c in data['compoundtypes']]

    def add_dataset(self, name, description, handle, cdt=None):
        """
        Adds a dataset to kive under the user associated
        with the token.

        :return: Dataset object
        """
        data = self._request('@api_dataset_add', 'POST', {
            'name': name,
            'description': description,
            'compound_datatype': cdt.id if cdt is not None else '__raw__'
        }, '', {
            'dataset_file': handle,
        })
        return Dataset(data['dataset'])

    def run_pipeline(self, pipeline, inputs):
        """
        Checks that a pipeline has the correct inputs, then
        submits the job to kive.

        :param pipeline: A Pipeline Object
        :param inputs: A list of Datasets
        :param force: Forces a job to be submitted without CDT checks.
        :return: A RunStatus object
        """

        # Check to see if we can even call this pipeline
        if len(inputs) != len(pipeline.inputs):
            return None

        post = {('input_%d' % i): d.dataset_id for (i, d) in enumerate(inputs)}
        post['pipeline'] = pipeline.pipeline_id

        data = self._request('@api_dataset_add', 'POST', post)
        return RunStatus(data)