"""

"""

class CompoundDatatype:

    def __init__(self, cdt):

        if cdt is None:
            self.cdt_id = '__raw__'
            self.name = 'Raw CDT'
        elif type(cdt) == dict:
            self.cdt_id = cdt['id']
            self.name = cdt['representation']
        else:
            self.cdt_id = cdt
            self.name = 'Unknown CDT'

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.cdt_id == other.cdt_id or self.name == other.name