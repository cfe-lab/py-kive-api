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
        self.url = obj['run_status']
        self.results_url = obj['run_results']
        self.api = api

    def _grab_stats(self):
        return self.api.get(self.url).json()

    def get_status(self):
        """
        Queries the server for the status of a run

        :return: A description string of the status
        """
        # TODO: Make change kive to return sane overall statuses
        status = self._grab_stats()['status']

        if status == '?':
            return "Waiting to start..."

        if '!' in status:
            return 'Run failed!'

        if '*' in status and '.' not in status:
            return 'Complete.'

        return 'Running...'

    def is_waiting(self):
        """
        Returns whether or not the run is queued
        on the server for processing.

        :return:
        """
        return self._grab_stats()['status'] == '?'

    def is_running(self):
        """
        Returns whether or not the run is running
        on the server

        :return:
        """
        status = self._grab_stats()['status']
        return '.' in status and '!' not in status

    def is_complete(self):
        """
        Returns whether or not the run has
        completed.

        :return:
        """
        status = self._grab_stats()['status']
        return ('.' not in status and status != '?') or '!' in status

    def is_successful(self):
        """
        Returns whether the run was successful,
        provided that it's also complete

        :return:
        """
        return '!' not in self._grab_stats()['status']

    def get_progress(self):
        """
        Gets the current run's progress bar

        :return:
        """
        return self._grab_stats()['status']

    def get_progress_percent(self):
        """
        Gets the current progress as a percentage.

        :return:
        """
        status = self._grab_stats()['status']
        return 100*float(status.count('*'))/float(len(status) - status.count('-'))

    def get_results(self):
        """
        Gets all the datasets that resulted from this
        pipeline (including intermediate results).

        :return: A list of Dataset objects, if the run is complete,
        an empty list otherwise
        """
        if not self.is_complete():
            return []

        datasets = self.api.get(self.results_url).json()
        return [Dataset(d, self.api) for d in datasets]