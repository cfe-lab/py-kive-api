"""
This module defines a wrapper for Kive's Dataset
object, and some support methods.
"""


class Dataset(object):
    """
    A wrapper class for Kive's Dataset object
    """

    def __init__(self, object):
        if type(object) == dict:
            self.dataset_id = object['id']
            self.name = object['name']
            self.url = object['download_url']
        else:
            self.dataset_id = object
            self.name = 'N/A'
            self.url = None

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Dataset (%d): "%s">' % (self.dataset_id, str(self))

    def download(self):
        """
        Downloads this dataset, creating a
        new file handle.

        :return: stream handle
        """
        pass