"""

"""
from .datatype import CompoundDatatype


class PipelineInput(object):
    """

    """

    def __init__(self, obj):
        self.dataset_idx = obj['dataset_idx']
        self.dataset_name = obj['dataset_name']
        self.compounddatatype = CompoundDatatype(obj['compounddatatype'])

    def __str__(self):
        return self.dataset_name

    def __unicode__(self):
        return self.dataset_name

    def __repr__(self):
        return '<Input (%d): %s (%s)>' % (self.dataset_idx, self.dataset_name, str(self.compounddatatype))


class Pipeline(object):
    """

    """

    def __init__(self, obj):
        if type(obj) == dict:
            self.pipeline_id = obj['id']
            self.revision_name = obj['revision_name']
            self.revision_number = obj['revision_number']
            self.inputs = [PipelineInput(i) for i in obj['inputs']]
        else:
            self.pipeline_id = object
            self.revision_number = None
            self.revision_name = None
            self.inputs = None
        print self.inputs

    def __str__(self):
        return '%s - rev %d' % (self.revision_name, self.revision_number) if self.revision_name is not None else 'N/A'

    def __unicode__(self):
        return str(self)

    def __repr__(self):
        return '<Pipeline (%d): %s>' % (self.pipeline_id, str(self))


class PipelineFamily(object):
    """

    """

    def __init__(self, obj):
        self.family_id = obj['id']
        self.name = obj['name']
        self.pipelines = [Pipeline(p) for p in obj['members']]
        self.published_version = Pipeline(obj['published_version']) if obj['published_version'] is not None else None

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __repr__(self):
        return '<Pipeline family (%d): %s>' % (self.family_id, str(self))
