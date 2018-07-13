from .base import DAO, API
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf


class InputTypeConfigurationOption(DAO):
    
    def __repr__(self):
        return "InputTypeConfigurationOption(human_name={}, description={})".format(
            self.human_name, self.description)


class InputType(DAO):
    
    def __repr__(self):
        return "InputType(name={}, type={})".format(self.name, self.system_type)


class InputTypeConfigOptionSchema(Schema):
    human_name=fields.String()
    additional_info=fields.Dict()
    description=fields.String(missing=None)
    default_value=fields.Raw(missing=None)
    attributes=fields.List(fields.String())
    option_type=fields.String(load_from='type', dump_to="type")
    is_optionsl=fields.Boolean()

    @post_load
    def make_obj(self, data):
        return InputTypeConfigurationOption(**data)


class InputTypeSchema(Schema):
    _path='system/inputs/types'
    _methods={
        "list": { "method": "GET", "path": "system/inputs/types/all", "dict": True}
    }

    system_type=fields.String(load_from='type', dump_to="type")
    name=fields.String()
    requested_configuration=fields.Dict(keys=fields.String(), values=InputTypeConfigOptionSchema())
    link_to_docs=fields.String()
    is_exclusive=fields.Boolean()

    @post_load
    def make_obj(self, data):
        ret=InputType(**data)
        if ret.requested_configuration:
            option_schema=InputTypeConfigOptionSchema(strict=True)
            for k in ret.requested_configuration.iterkeys():
                ret.requested_configuration[k]=option_schema.load(ret.requested_configuration[k]).data

        return ret


class Input(DAO):
    
    def __repr__(self):
        return "Input(title={}, id={}, global_flag={}, node={}, type={})".format(
            self.title,
            self.id if hasattr(self, 'id') else 'None',
            self.global_flag,
            self.node,
            self.input_type)


class InputSchema(Schema):
    _path="system/inputs"
    _methods={
        "list": {"method": "GET", 'field': 'inputs'},
        "get": {"method": "GET", "path": "system/inputs/{id}"},
        "add": {"method": "POST"},
        "update": {"method": "PUT", "path": "system/inputs/{id}"},
        "delete": { "method": "DELETE", "path": "system/inputs/{id}"}
    }

    title=fields.String()
    global_flag=fields.Boolean(load_from="global", dump_to="global")
    name=fields.String()
    content_pack=fields.String(allow_none=True)
    created_at=fields.DateTime()
    input_type=fields.String(load_from="type", dump_to="type")
    creator_user_id=fields.String()
    node=fields.String(allow_none=True)
    id=fields.String()
    attributes=fields.Dict(allow_none=True)
    configuration=fields.Dict(allow_none=True)

    @post_load
    def make_obj(self, data):
        return Input(**data)


def _get_input_types(self):
    if not hasattr(self, '_input_types') or self._input_types is None:
        self._input_types=self.input_types.list()
    return self._input_types


class Extractor(DAO):

    def __repr__(self):
        return "Extractor(id={}, title={})".format(self.id, self.title)


class ExtractorSchema(Schema):
    _path=u"system/inputs/{input_id}/extractors"
    _methods={
        "list": {"method": "GET", 'field': 'extractors'},
        "get": {"method": "GET", "path": "system/inputs/{input_id}/extractors/{id}"},
        "add": {"method": "POST", "get_attr_map": {"extractor_id": "id"}},
        "update": { "method": "PUT", 
                    "path": "system/inputs/{input_id}/extractors/{id}", 
                    "get_attr_map": {"extractor_id": "id"}},
        "delete": { "method": "DELETE", "path": "system/inputs/{input_id}/extractors/{id}"}
    }
    _types=('copy_input', 'grok', 'json', 'regex', 'regex_replace', 'split_and_index', 'substring', 'lookup_table')

    id=fields.String(load_only=True)
    title=fields.String()
    extractor_type=fields.String(load_from="type", validate=OneOf(_types))
    converter_status=fields.List(fields.Dict(), missing=[], load_from='converters', load_only=True)
    converter_def=fields.Dict(dump_to='converters', missing={})
    order=fields.Integer()
    cut_or_copy=fields.String(load_from='cursor_strategy', validate=OneOf(("cut", "copy")))
    source_field=fields.String()
    target_field=fields.String(missing="")
    extractor_config=fields.Dict(missing={})
    creator_user_id=fields.String(load_only=True)
    condition_type=fields.String(missing="none")
    condition_value=fields.String(missing="")

    @post_load
    def make_obj(self, data):
        return Extractor(**data)


class IndexRetention(DAO):

    def __repr__(self):
        return "IndexRetention(type_name={})".format(self.type_name)


class IndexRetentionSchema(Schema):
    _path="system/indices/retention/strategies"
    _methods={
        "list": { "method": "GET", "field": "strategies" }
    }
    type_name=fields.String(load_from="type", dump_to="type")
    default_config=fields.Dict()
    json_schema=fields.Dict()

    @post_load
    def make_obj(self, data):
        return IndexRetention(**data)


class IndexRotation(DAO):
    
    def __repr__(self):
        return "IndexRotation(type_name={})".format(self.type_name)


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


class IndexSet(DAO):
    
    def __repr__(self):
        ret="IndexSet(id={}, title={}, index_prefix={}, "+\
            "shards={}, replicas={}, rotation_strategy={}, retention_strategy={}, creation_date={}," +\
            "index_analyzer={}, index_optimization_max_num_segments={}, index_optimization_disabled={}, "+\
            "writable={}, default={})"
        return ret.format(
            self.id, self.title, self.index_prefix, self.shards, self.replicas, self.rotation_strategy_class,
            self.retention_strategy_class, self.creation_date, self.index_analyzer,
            self.index_optimization_max_num_segments, self.index_optimization_disabled, self.writable, self.default
        )


class IndexSetSchema(Schema):
    _path="system/indices/index_sets"
    _methods={
        "list": { "method": "GET", "field": "index_sets" },
        "get": {"method": "GET", "path": "system/indices/index_sets/{id}"},
        "add": {"method": "POST"},
        "update": {"method": "PUT", "path": "system/indices/index_sets/{id}"},
        "set_default": {"method": "PUT", "path": "system/indices/index_sets/{id}/default" },
        "delete": { "method": "DELETE", "path": "system/indices/index_sets/{id}"}
    }
    id=fields.String()
    title=fields.String()
    description=fields.String(allow_none=True)
    index_prefix=fields.String()
    shards=fields.Integer()
    replicas=fields.Integer()
    rotation_strategy_class=fields.String()
    rotation_strategy=fields.Dict()
    retention_strategy_class=fields.String()
    retention_strategy=fields.Dict()
    creation_date=fields.DateTime()
    index_analyzer=fields.String(default="standard")
    index_optimization_max_num_segments=fields.Integer(default=1)
    index_optimization_disabled=fields.Boolean(default=False)
    writable=fields.Boolean(default=True)
    default=fields.Boolean(default=False)

    @post_load
    def make_obj(self, data):
        return IndexSet(**data)


class LDAPConfig(DAO):
    pass


class LDAPConfigSchema(Schema):
    _path="system/ldap/settings"
    _methods={
        "get": {"method": "GET"},
        "add": {"method": "PUT"},
        "update": {"method": "PUT"},
        "delete": {"method": "DELETE"}
    }
    enabled=fields.Boolean()
    system_username=fields.String()
    system_password=fields.String()
    ldap_uri=fields.String()
    use_start_tls=fields.Boolean(missing=False)
    trust_all_certificates=fields.Boolean(missing=False)
    active_directory=fields.Boolean()
    search_base=fields.String()
    search_pattern=fields.String()
    default_group=fields.String()
    group_mapping=fields.Dict(allow_none=True)
    group_search_base=fields.String(allow_none=True)
    group_id_attribute=fields.String(allow_none=True)
    group_search_pattern=fields.String(allow_none=True)
    display_name_attribute=fields.String(allow_none=True)
    additional_default_groups=fields.List(fields.String(), missing=[], allow_none=True)

    @post_load
    def make_obj(self, data):
        return LDAPConfig(**data)


class Stream(DAO):
    
    def __repr__(self):
        return "Stream(id={}, title={})".format(self.id, self.title)


class AlertReceiverSchema(Schema):
    emails=fields.List(fields.String(), missing=[], allow_none=True)  # FIXME:
    users=fields.List(fields.String(), missing=[], allow_none=True)  # FIXME:


class StreamSchema(Schema):
    _path="streams"
    _methods={
        "list": { "method": "GET", "field": "streams" },
        "get": {"method": "GET", "path": "streams/{id}"},
        "add": {"method": "POST", "get_attr_map": {"stream_id": "id"}},
        "update": {"method": "PUT", "path": "streams/{id}"},
        "delete": {"method": "DELETE", "path": "streams/{id}"},
        "resume": {"method": "POST", "path": "streams/{id}/resume", "no_get": True}
    }
    id=fields.String()
    creator_user_id=fields.String()
    outputs=fields.List(fields.String(), missing=[], allow_none=True)    # FIXME: 
    matching_type=fields.String(missing="AND", validate=OneOf(('AND', 'OR')))
    description=fields.String(allow_none=True)
    created_at=fields.DateTime()
    disabled=fields.Boolean()
    rules=fields.List(fields.Dict(), missing=[], allow_none=True)  # FIXME: Rules are almost certainly structured
    alert_conditions=fields.List(fields.Dict(), missing=[], allow_none=True)  # FIXME:
    alert_receivers=fields.Nested(AlertReceiverSchema)
    title=fields.String()
    content_pack=fields.String(allow_none=True)
    remove_matches_from_default_stream=fields.Boolean(missing=False)
    index_set_id=fields.String()
    is_default=fields.Boolean(missing=True)

    @post_load
    def make_obj(self, data):
        return Stream(**data)


class StreamRuleType(DAO):

    def __repr__(self):
        return "StreamRuleType(id={}, name={}, short={})".format(self.id, self.name, self.short_desc)


class StreamRuleTypeShema(Schema):
    _path="streams/{stream_id}/rules/types"
    _methods={
        "list": { "method": "GET", "list": True}
    }

    id=fields.Integer()
    name=fields.String()
    short_desc=fields.String()
    long_desc=fields.String()

    @post_load
    def make_obj(self, data):
        return StreamRuleType(**data)


class StreamRule(DAO):

    def __repr__(self):
        return "StreamRule(id={}, stream_id={}, description={})".format(self.id, self.stream_id, self.description)


class StreamRuleSchema(Schema):
    _path="streams/{stream_id}/rules"
    _methods={
        "list": { "method": "GET", "field": "stream_rules" },
        "get": {"method": "GET", "path": "streams/{stream_id}/rules/{id}"},
        "add": {"method": "POST", "get_attr_map": {"streamrule_id": "id"}, "filter_attr": ('stream_id',) },
        "update": {"method": "PUT", "path": "streams/{stream_id}/rules/{id}", "filter_attr": ('stream_id', 'id') },
        "delete": {"method": "DELETE", "path": "streams/{stream_id}/rules/{id}"}
    }

    id=fields.String()
    stream_id=fields.String()
    description=fields.String(missing="", allow_none=True)
    field=fields.String()
    _type=fields.Integer(load_from="type", dump_to="type")
    inverted=fields.Boolean(missing=False)
    value=fields.String(missing="")

    @post_load
    def make_obj(self, data):
        return StreamRule(**data)


class Role(DAO):
    
    def __repr__(self):
        return "Role(name={}, description={})".format(self.name, self.description)


class RoleSchema(Schema):
    _path="roles"
    _methods={
        "list": { "method": "GET", "field": "roles" },
        "get": {"method": "GET", "path": "roles/{name}"},
        "add": {"method": "POST"},
        "update": {"method": "PUT", "path": "roles/{name}"},
        "delete": {"method": "DELETE", "path": "roles/{name}"},
    }

    name=fields.String()
    description=fields.String(allow_none=True)
    permissions=fields.List(fields.String(), missing=[])
    read_only=fields.Boolean(missing=False)

    @post_load
    def make_obj(self, data):
        return Role(**data)


class SearchResults(DAO):
    pass


class DictMessage(DAO):
 
    def __repr__(self):
        def attr_filter(x):
            return not (callable(x) or (isinstance(x, basestring) and x.startswith('__')))

        # import pdb ; pdb.set_trace()
        ret=self.NAME+'('

        for (i, k) in enumerate(filter(attr_filter, sorted(self.__dict__.keys()))):
            if i>0: 
                ret=ret+", "
            ret=ret+"{}={}".format(k, getattr(self, k))
        ret=ret+')'
        return ret


class ELMessage(DictMessage):
    NAME="ELMessage"


class GL2Message(DictMessage):
    NAME="GL2Message"


class SearchSchema(Schema):
    _methods={
        "list": { "method": "GET" }
    }

    query=fields.String()
    built_query=fields.String()
    used_indices=fields.List(fields.Dict(), missing=[], allow_none=True)
    messages=fields.List(fields.Dict(), missing=[], allow_none=True)
    message_fields=fields.List(fields.String(), missing=[], allow_none=True, load_from="fields")
    time=fields.Integer()
    total_results=fields.Integer()
    _from=fields.DateTime(load_from="from")
    to=fields.DateTime()
    decoration_stats=fields.Dict(allow_none=True)

    @post_load
    def make_obj(self, data):

        def make_message(data):
            el_message=ELMessage(**data)
            if 'message' in data:
                el_message.message=GL2Message(**el_message.message)
            return el_message

        res=SearchResults(**data)
        res.messages=map(make_message, res.messages)
        return res


class RelativeSearchSchema(SearchSchema):
    _path="search/universal/relative"


class AbsoluteSearchSchema(SearchSchema):
    _path="search/universal/absolute"


_api=None


def get_api(url=None, *args, **kwargs):
    """Initialize and return the api object.

    The method  initializes a singleton API object and returns it. The API
    object contains endpoint variables for supported schemas.
    """
    global _api
    if _api is None and url is not None:
        _api=API(url, *args, **kwargs)
        _api.add_resource(name="inputs", schema=InputSchema)
        _api.add_resource(name="input_types", schema=InputTypeSchema)
        _api.add_resource(name='index_retention_strategies', schema=IndexRetentionSchema)
        _api.add_resource(name='index_rotation_strategies', schema=IndexRotationSchema)
        _api.add_resource(name='index_sets', schema=IndexSetSchema)
        _api.add_resource(name="ldap_config", schema=LDAPConfigSchema)
        _api.add_resource(name='streams', schema=StreamSchema)
        _api.add_resource(name='stream_rule_types', schema=StreamRuleTypeShema)
        _api.add_resource(name="stream_rules", schema=StreamRuleSchema)
        _api.add_resource(name='roles', schema=RoleSchema)
        _api.add_resource(name='extractors', schema=ExtractorSchema)
        _api.add_resource(name='relative_search', schema=RelativeSearchSchema)
        _api.add_resource(name='absolute_search', schema=AbsoluteSearchSchema)
        _api.get_input_types=_get_input_types.__get__(_api, API)
    return _api


