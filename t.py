from gl2api import get_api, AbsoluteSearchSchema

if __name__ == '__main__':
    from requests.auth import HTTPBasicAuth
    auth=HTTPBasicAuth("admin", "Geheim")

    api=get_api("http://microserver-core:9000/api", auth=auth)
    obj_api=api.stream_rules
    
    INPUT_ID="5abe921c08813b00011123f3"
    INDEX_SET_ID="5abe920408813b00011123a1"
    STREAM_ID="5b070a87e64ada000110ac23"

    from gl2api.search import get_stream_by_name
    get_stream_by_name('Virus Scans')

    schema=AbsoluteSearchSchema()
    obj=api.absolute_search.list(query_params={
        # 'filter': "stream={}".format(STREAM_ID), 
        'from': '2018-07-01T04:00:00.000Z',
        'to': '2018-07-02T04:00:00.000Z',
        'query': "level:<=3"})
    print("Result: {}".format(schema.dump(obj).data))
    for r in obj.messages:
        print "   {}".format(r)

    # obj_list=obj_api.list(stream_id=STREAM_ID)
    # for (i, s) in enumerate(obj_list, 1):
    #     print("list {}: {}".format(i, s))

    # if len(obj_list)>=1:
    #     s=obj_api.get(id=obj_list[0].id, stream_id=STREAM_ID)
    #     print("Get {}: {}".format(s, schema.dump(s).data))

    # obj=StreamRule(
    #     stream_id=STREAM_ID,
    #     description="xyz is present",
    #     _type=5,
    #     field="xyz",
    #     value=""
    # )
    # new_obj=obj_api.add(obj)
    # print("Added: {}".format(new_obj))

    # new_obj.inverted=True

    # new_obj=obj_api.update(new_obj)
    # print("Updated: {}".format(new_obj))

    # obj_api.delete(new_obj)
    # print("Deleted {}".format(new_obj))
