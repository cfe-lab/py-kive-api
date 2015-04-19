"""
This module defines a wrapper for Kive's Dataset
object, and some support methods.
"""
from .datatype import CompoundDatatype
import requests


class Dataset(object):
    """
    A wrapper class for Kive's Dataset object
    """

    def __init__(self, obj, api=None):
        if type(obj) == dict:
            self.dataset_id = obj['id']
            self.name = obj['name']
            self.url = obj['download_url']
            self.cdt = CompoundDatatype(obj['compounddatatype'])
        else:
            self.dataset_id = obj
            self.name = 'N/A'
            self.url = None
            self.cdt = CompoundDatatype(None)
        self.api = api

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Dataset (%s): "%s" (%s)>' % (self.dataset_id, str(self), str(self.cdt))

    def download(self, handle):
        """
        Downloads this dataset, creating a
        new file handle.

        :return: stream handle
        """

        response = requests.get(self.api.server_url + self.url, stream=True)

        if not response.ok:
            return None

        for block in response.iter_content(1024):
            if not block:
                break
            handle.write(block)
