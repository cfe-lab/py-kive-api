"""
This module defines a wrapper for Kive's Dataset
object, and some support methods.
"""
from .datatype import CompoundDatatype
from . import KiveServerException, KiveAuthException, KiveMalformedDataException


class Dataset(object):
    """
    A wrapper class for Kive's Dataset object
    """

    def __init__(self, obj, api=None):
        try:
            if type(obj) == dict:
                self.dataset_id = obj['id']
                self.symbolicdataset_id = obj.get('symbolic_id', None)
                self.filename = obj['filename']
                self.name = obj['name'] if 'name' in obj else obj['output_name']
                self.cdt = CompoundDatatype(obj['compounddatatype']) if 'compounddatatype' in obj else None

        except (ValueError, IndexError, KeyError):
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

        response = self.api.get("@api_dataset_dl",
                                context={'dataset-id': self.dataset_id},
                                is_json=False)

        if 400 <= response.status_code < 499:
            raise KiveAuthException("Authentication failed for download (%s)!" % self.url)
        if not response.ok:
            raise KiveServerException("Server error downloading file (%s)!" % self.url)

        for block in response.iter_content(1024):
            if not block:
                break
            handle.write(block)

    def read(self):
        """
        Returns an iterator to data set
        :return:
        """
        response = self.api.get("@api_dataset_dl",
                                context={'dataset-id': self.dataset_id},
                                is_json=False)

