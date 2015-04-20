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

    def _grab_stats(self):
        return self.api._request(self.url)['run']

    def get_status(self):
        """
        Queries the server for the status
        of a run

        :return: A dictionary with keys 'status' and
                'progress'
        """
        status = self._grab_stats()['status']

        if status == '?':
            return "Waiting to start..."

        if '!' in status:
            return 'Run failed!'

        if '*' in status and '.' not in status:
            return 'Complete.'

        return 'Running...'

    def is_waiting(self):
        return self._grab_stats()['status'] == '?'

    def is_running(self):
        status = self._grab_stats()['status']
        return '.' in status and '!' not in status

    def is_complete(self):
        status = self._grab_stats()['status']
        return ('.' not in status and status != '?') or '!' in status

    def is_successful(self):
        return '!' not in self._grab_stats()['status']

    def get_progress(self):
        return self._grab_stats()['status']

    def get_progress_percent(self):
        status = self._grab_stats()['status']
        return 100*float(status.count('*'))/float(len(status) - status.count('-'))

    def get_results(self):
        """
        Gets all the datasets that resulted from this
        pipeline (including intermediate results).

        :return: A list of Dataset objects
        """
        if not self.is_complete():
            return None

        resurl = self.api._request(self.url)['results']
        datasets = self.api._request(resurl[1:])['results']
        return [Dataset(d, self.api) for d in datasets]