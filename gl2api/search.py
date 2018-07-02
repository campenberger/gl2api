from . import get_api


class ObjectNotFound(Exception):
    """Exception when a graylog object can't be found."""

    def __init__(self, type, name=None, id=None):
        if name is not None:
            super(ObjectNotFound, self).__init__("No {} object with name {} found".format(type, name))
        elif id is not None:
            super(ObjectNotFound, self).__init__("No {} object with id {} found".format(type, id))
        else:
            super(ObjectNotFound, self).__init__("{} not found".format(type))


def get_object_by_name(list_api, name, object_name):
    sets=filter(
        lambda inp: inp.title==name,
        list_api.list())
    if len(sets)==0:
        raise ObjectNotFound(object_name, name)
    else:
        return sets[0]


def get_stream_by_name(name):
    return get_object_by_name(get_api().streams, name, "Stream")


def get_index_set_by_name(name):
    return get_object_by_name(get_api().index_sets, name, 'IndexSet')
