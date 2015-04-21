"""
This module defines a wrapper for Kive's Dataset
object, and some support methods.
"""
from .datatype import CompoundDatatype
from . import KiveServerException, KiveAuthException, KiveMalformedDataException
import requests


class Dataset(object):
    """
    A wrapper class for Kive's Dataset object
    """

    def __init__(self, obj, api=None):
        try:
            if type(obj) == dict:
                self.dataset_id = obj['id']
                self.name = obj['name']
                self.url = obj['download_url']
                self.cdt = CompoundDatatype(obj['compounddatatype'])
                self.filename = obj['filename']
            else:
                self.dataset_id = obj
                self.name = 'N/A'
                self.url = None
                self.cdt = CompoundDatatype(None)
        except (ValueError, IndexError):
            raise KiveMalformedDataException(
                'Server gave malformed Dataset object:\n%s' % obj
            )
        self.api = api

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Dataset (%s): "%s" (%s)>' % (self.dataset_id, str(self), str(self.cdt))

    def download(self, handle):
        """
        Downloads this dataset and streams it into handle

        :param handle: A file handle
        """

        headers = {'Authorization': 'Token %s' % self.api.token}
        response = requests.get(self.api.server_url + self.url[1:], stream=True, headers=headers)

        if 400 <= response.status_code < 499:
            raise KiveAuthException("Authentication failed for download (%s)!" % self.url)
        if not response.ok:
            raise KiveServerException("Server error downloading file (%s)!" % self.url)

        for block in response.iter_content(1024):
            if not block:
                break
            handle.write(block)
