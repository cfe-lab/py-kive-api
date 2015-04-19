"""
This module defines a class that keeps track
of a run in Kive.
"""
from .dataset import Dataset


class RunStatus(object):
    """
    This keeps track of a run in Kive, there's no immediate
    mapping of this object to an object in Kive, except maype
    RunToProcess
    """

    def __init__(self, obj, api):
        self.rtp_id = obj['id']
        self.url = obj['run_status'][1:]
        self.api = api

    def get_status(self):
        """
        Queries the server for the status
        of a run

        :return: A dictionary with keys 'status' and
                'progress'
        """
        response = self.api._request(self.url)

        if response is None:
            return {
                'status': 'Waiting to start',
                'progress': '?',
            }

        # When complete, get url to get results
        return {
            'status': 'In progress',
            'progress': '-'
        }


    def get_results(self):
        """
        Gets all the datasets that resulted from this
        pipeline (including intermediate results).

        :return: A list of Dataset objects
        """
        if self.get_status()['status'] != 'Complete':
            return None

        datasets = self.api._request(self.datasets_url)
        return [Dataset(d) for d in datasets]