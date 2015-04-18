
class Dataset(object):

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
        pass