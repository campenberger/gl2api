import re
import time
import requests
import requests.exceptions
import urllib
from functools import partial
from marshmallow import fields
from marshmallow.marshalling import Unmarshaller
from .util import loggingFactory

_getLogger=loggingFactory()


class DAO(object):

    def __init__(self, **kwargs):
        self._dirty_fields=set()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                if getattr(self, k)!=v:
                    setattr(self, k, v)
                    self._dirty_fields.add(k)
            else:
                setattr(self, k, v)
                self._is_dirty=True
                self._dirty_fields.add(k)

    def is_dirty(self):
        return len(self._dirty_fields)>0

    def is_field_dirty(self, field_name):
        return field_name in self._dirty_fields


class Resource(object):
    pass


class InvalidMethodType(Exception):
    pass


class ApiError(Exception):
    
    def __init__(self, r):
        msg="Request error: status_code={} text={}".format(r.status_code, r.text)
        super(ApiError, self).__init__(msg)


class API(object):
    """The very simple JIRA API.

    It supports generic handling for put, post, get and delete requests that
    can be utilized in methods for a resource. The method add_resource is used
    to add all method a particular resource entry points support to API.

        class IndexRotationSchema(Schema):
            _path="system/indices/rotation/strategies"
            _methods={
                "list": { "method": "GET", "field": "strategies" }
            }
            type_name=fields.String(load_from="type", dump_to="type")
            default_config=fields.Dict()
            json_schema=fields.Dict()

            @post_load
            def make_obj(self, data):
                return IndexRotation(**data)

        api=API("http://localhost:9000/api", auth=auth)
        api.add_resource(name='index_rotation_strategies', schema=IndexRotationSchema)

        api.index_rotation_strategies.list()
    """
    
    RETRY=10

    def __init__(self, root_url, timeout=10, auth=None):
        self.root_url=root_url
        self.timeout=timeout
        self.auth=auth

    @staticmethod
    def retry(m, *args, **kwargs):
        for i in xrange(0, 12):
            try:
                return m(*args, **kwargs)
            except requests.exceptions.ConnectionError:
                if i<2:
                    _getLogger().warn('Connection error, retrying in %ds', API.RETRY)
                    time.sleep(API.RETRY)
                else:
                    _getLogger().error('Unable to connect')
                    raise


    def _makeUrl(self, method_info, Object_Schema, kwargs): # noqa
        url=method_info['path'] if 'path' in method_info else Object_Schema._path
        url=self.root_url+'/'+url
        if 'query_params' in kwargs:
            url=url+'?'+urllib.urlencode(kwargs['query_params'])
            del kwargs['query_params']
        return url.format(**kwargs)

    def _get(self, ObjectSchema, method_info, timeout=2.0, auth=None, *args, **kwargs): # noqa
        headers={"Accept": "application/json"}
        url=self._makeUrl(method_info, ObjectSchema, kwargs)
        r=API.retry(requests.get, url, timeout=timeout, auth=auth, headers=headers)
        unmarshaller=Unmarshaller()
        if r.status_code==200:
            # if we have a field, in the method info, we expect a collection back
            # otherwise it will be just a single element
            if 'field' in method_info:
                field_def={
                    method_info['field']: fields.Nested(ObjectSchema, many=True),
                    "total": fields.Integer()
                }
                data=unmarshaller(r.json(), field_def, partial=True)
                return data[method_info['field']]

            elif 'dict' in method_info and method_info['dict']:
                data=r.json()
                schema=ObjectSchema(strict=True)
                ret={
                    k: schema.load(v).data
                    for (k, v) in data.iteritems()
                }
                return ret

            elif 'list' in method_info and method_info['list']:
                data=r.json()
                schema=ObjectSchema(strict=True)
                return [schema.load(v).data for v in data]
                
            else:
                data=ObjectSchema(strict=True).load(r.json())
                return data.data

        else:
            r.raise_for_status()

    @staticmethod
    def _args_from_path(path, obj, kwargs):
        if isinstance(obj, dict):
            _hasattr=lambda x: x in obj     # noqa
            _getattr=lambda x: obj[x]       # noqa
        else:
            _hasattr=lambda x: hasattr(obj, x)  # noqa
            _getattr=lambda x: getattr(obj, x)  # noqa

        for field in re.findall(r'\{.*?\}', path):
            field=field[1:-1]
            if _hasattr(field):
                kwargs[field]=_getattr(field)

    def _put_post(self, obj, Schema, method_info, timeout=2.0, auth=None, is_post=True, *args, **kwargs):   # noqa
        headers={"Content-Type": "application/json"}
        data=Schema().dump(obj)

        # construct URL from path, given object and kwargs        
        _kwargs={}
        path=method_info['path'] if 'path' in method_info else Schema._path
        API._args_from_path(path, obj, _kwargs)
        _kwargs.update(kwargs)

        # remove attributes that shouldn't be posted
        if 'filter_attr' in method_info:
            for a in method_info['filter_attr']:
                del data.data[a]

        url=self._makeUrl(method_info, Schema, _kwargs)
        if is_post:
            r=API.retry(requests.post, url, json=data.data, timeout=timeout, auth=auth, headers=headers)
        else:
            r=API.retry(requests.put, url, json=data.data, timeout=timeout, auth=auth, headers=headers)
        if r.status_code in (200, 201):
            data=r.json()
            _getLogger('_put_post').debug("Saved object, received: {}".format(data))
            if 'get' in Schema._methods and ('no_get' not in method_info or not method_info['no_get']):
                if 'get_attr_map' in method_info:
                    for (k, v) in method_info['get_attr_map'].iteritems():
                        try:
                            _kwargs[v]=data[k]
                        except KeyError:
                            pass
                else:
                    API._args_from_path(path, data, _kwargs)

                path=Schema._methods['get']['path'] if 'path' in Schema._methods['get'] else Schema._path
                return self._get(Schema, Schema._methods['get'], timeout, auth, **_kwargs)
            else:
                return data
        elif r.status_code==204:
            if 'get' in Schema._methods and ('no_get' not in method_info or not method_info['no_get']):
                return self._get(Schema, Schema._methods['get'], timeout, auth)
            else:
                return data
        elif r.status_code>=400 and r.status_code<500:
            raise ApiError(r)
        else:
            # import pdb ; pdb.set_trace()
            _getLogger('_put_post').error('Status code error {}: {}'.format(r.status_code, r.text))
            r.raise_for_status()

    def _delete(self, obj, Schema, method_info, timeout=2.0, auth=None, *args, **kwargs):   # noqa
        headers={"Content-Type": "application/json"}

        # update kwargs with the path attributes required to identify the obj
        _kwargs={}
        _kwargs.update(kwargs)
        path=method_info['path'] if 'path' in method_info else Schema._path
        for field in re.findall(r'\{.*?\}', path):
            field=field[1:-1]
            if hasattr(obj, field):
                _kwargs[field]=getattr(obj, field)

        url=self._makeUrl(method_info, Schema, _kwargs)
        r=API.retry(requests.delete, url, timeout=timeout, auth=auth, headers=headers)
        if r.status_code==204:
            return True
        elif r.status_code>=400 and r.status_code<500:
            raise ApiError(r)
        else:
            r.raise_for_status()            

    def add_resource(self, name, schema):
        """Configure the resource endpoint <name> based on the given <schema>."""
        res=Resource()
        for (fname, info) in schema._methods.items():
            if callable(info):
                setattr(res, fname, info)
            elif info['method']=='GET':
                setattr(res, fname, 
                        partial(self._get, ObjectSchema=schema, method_info=info, timeout=self.timeout, auth=self.auth))
            elif info['method']=='POST':
                setattr(res, fname, 
                        partial(self._put_post, Schema=schema, method_info=info, timeout=self.timeout, auth=self.auth,
                                is_post=True))
            elif info['method']=='PUT':
                setattr(res, fname, 
                        partial(self._put_post, Schema=schema, method_info=info, timeout=self.timeout, auth=self.auth, 
                                is_post=False))
            elif info['method']=='DELETE':
                setattr(res, fname, 
                        partial(self._delete, Schema=schema, method_info=info, timeout=self.timeout, auth=self.auth))
            else:
                raise InvalidMethodType("Method: {}, type: {}".format(name, info['method']))
        setattr(self, name, res)


class ObjectNotFound(Exception):
    """Exception when a graylog object can't be found."""

    def __init__(self, type, name=None, id=None):
        if name is not None:
            super(ObjectNotFound, self).__init__("No {} object with name {} found".format(type, name))
        elif id is not None:
            super(ObjectNotFound, self).__init__("No {} object with id {} found".format(type, id))
        else:
            super(ObjectNotFound, self).__init__("{} not found".format(type))


class UpdateField(object):
    """Data class to describe the conversion a resource field.

    The class constains the neccessary information to convert a field to and from and object
    to a API resource attribute
    """

    def __init__(self, name, attribute_name=None, converter=None, default=None):
        self.name=name
        self.attribute_name=attribute_name
        self.converter=converter
        self.default=default



