"""
Contains the main class for the accessing Kive's
RESTful API.

"""
from .dataset import Dataset
from .pipeline import PipelineFamily
from .datatype import CompoundDatatype
from .runstatus import RunStatus

import requests

from . import KiveMalformedDataException, KiveAuthException, KiveServerException

class KiveAPI(object):
    """
    The main KiveAPI class
    """
    SERVER_URL = ""
    AUTH_TOKEN = ""

    def __init__(self, server=None, token=None):
        self.server_url = server
        self.token = token

        if server is None:
            self.server_url = KiveAPI.SERVER_URL

        if token is None:
            self.token = KiveAPI.AUTH_TOKEN

        if self.server_url[-1] != '/':
            self.server_url += "/"

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
            'api_pipelines_get_runs': 'api/pipelines/get-active-runs/',
        }

    @staticmethod
    def get_token(url, username, password):
        """
        Queries the server at url for an authentication token.

        :param url: Url to the Kive server
        :param username: Username
        :param password: Password
        :return: A string representation of the user's token
        """
        response = requests.post(url + "api/token-auth/", {'username': username, 'password': password})

        try:
            return response.json()['token']
        except (ValueError, KeyError):
            return None

    def _request(self, endpoint, method='GET', data={}, files={}):
        """
        Internal functions used to form common requests
        to endpoints

        :param endpoint: Endpoint url
        :return: json dictionary of results
        """

        # If we have a quick @ tag, lookup the
        if endpoint[0] == '@':
            endpoint = self.endpoint_map[endpoint[1:]]

        headers = {'Authorization': 'Token %s' % self.token}

        # Choose method
        if method.upper() == 'GET':
            response = requests.get(self.server_url + endpoint, headers=headers)
        else:
            response = requests.post(self.server_url + endpoint, data, headers=headers, files=files)
        try:

            if 500 <= response.status_code < 599:
                raise KiveServerException("Server 500 error (check server config '%s!')" % self.server_url)

            json_data = response.json()
            if 400 <= response.status_code < 499:
                if 'detail' in json_data:
                    raise KiveAuthException("Couldn't authorize request (%s)" % json_data['detail'])
                raise KiveAuthException("Couldn't authorize request!")

            else:
                return json_data
        except ValueError:
            raise KiveMalformedDataException("Malformed response from server! (check server config '%s!')" % self.server_url)

    def get_datasets(self):
        """
        Returns a list of all datasets.

        :return: A list of Dataset objects.
        """
        data = self._request('@api_get_dataset')
        datasets = {'result': data['datasets']}

        def _load_more(next, dats):
            if next is not None:
                more = self._request(next[1:])
                dats['result'] += more['datasets']
                _load_more(more['next'], dats)

        _load_more(data['next'], datasets)

        return [Dataset(d, self) for d in datasets['result']]

    def get_dataset(self, dataset_id):
        """
        Gets a dataset in kive by its ID.

        :param dataset_id: Integer id
        :return: Dataset object
        """

        # TODO: Make a new API route in Kive that looks up a specific ID
        return self.find_datasets(dataset_id=dataset_id)[0]

    def find_datasets(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        datasets = self.get_datasets()
        ret = []
        if 'dataset_id' in kwargs:
            ret += filter(lambda d: d.dataset_id == kwargs['dataset_id'], datasets)

        if 'dataset_name' in kwargs:
            ret +=  filter(lambda d: d.name == kwargs['dataset_name'], datasets)

        return ret

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

    def get_pipeline_family(self, pipeline_fam_id):
        """
        Returns a PipelineFamily object for a specific id

        :param pipeline_fam_id:
        :return: PipelineFamily Object
        """
        all_pf = self.get_pipeline_families()
        try:
            return filter(lambda x: x.family_id == pipeline_fam_id, all_pf)[0]
        except IndexError:
            return None

    def get_pipeline(self, pipeline_id):
        """

        :param pipeline_id:
        :return: Pipeline object
        """
        all_pf = self.get_pipeline_families()
        for pf in all_pf:
            valid = filter(lambda p: p.pipeline_id == pipeline_id, pf.pipelines())
            if len(valid) >= 1:
                return valid[0]
        return None

    def get_cdts(self):
        """
        Returns a list of all current compound datatypes

        :return: A list of CompoundDatatypes objects
        """

        data = self._request('@api_get_cdts')
        return [CompoundDatatype(c) for c in data['compoundtypes']]

    def get_cdt(self, cdt_id):
        """
        Returns a CDT object for a specific id.

        :param cdt_id:
        :return: CompoundDatatype object.
        """
        data = self.get_cdts()
        try:
            return filter(lambda c: cdt_id == c.cdt_id, data)
        except IndexError:
            return None

    def add_dataset(self, name, description, handle, cdt=None):
        """
        Adds a dataset to kive under the user associated
        with the token.

        :return: Dataset object
        """

        data = self._request('@api_dataset_add', 'POST', {
            'name': name,
            'description': description,
            'compound_datatype': cdt.cdt_id if cdt is not None else '__raw__'
        }, {
            'dataset_file': handle,
        })
        return Dataset(data['dataset'], self)

    def run_pipeline(self, pipeline, inputs, force=False):
        """
        Checks that a pipeline has the correct inputs, then
        submits the job to kive.

        :param pipeline: A Pipeline Object
        :param inputs: A list of Datasets
        :return: A RunStatus object
        """

        # Check to see if we can even call this pipeline
        if len(inputs) != len(pipeline.inputs):
            raise KiveMalformedDataException(
                'Number of inputs to pipeline is not equal to the number of given inputs (%d != %d)' % (
                    len(inputs),
                    len(pipeline.inputs)
                )
            )

        if not force:
            # Check to see if the CDT for each input matches the
            # Expected CDT
            zlist = zip(inputs, pipeline.inputs)

            for dset, pi in zlist:
                if dset.cdt != pi.compounddatatype:
                    raise KiveMalformedDataException(
                        'Content check failed (%s != %s)! ' % (str(dset.cdt), str(pi.compounddatatype))
                    )

        # Construct the inputs
        post = {('input_%d' % (i+1)): d.dataset_id for (i, d) in enumerate(inputs)}
        post['pipeline'] = pipeline.pipeline_id

        data = self._request('@api_pipelines_startrun', 'POST', post)
        return RunStatus(data['run'], self)