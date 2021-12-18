import requests
import pprint

#####################
PUT /foo/bar/_bulk
{ "index" : { "_id" : "1" } }
{ "foo" : "bar" }
{ "index" : { "_id" : "2" } }
{ "foo" : "bar" }
{ "index" : { "_id" : "3" } }
{ "foo" : "baz" }
{ "index" : { "_id" : "4" } }
{ "foo" : "spam" }
{ "index" : { "_id" : "5" } }
{ "foo" : "spam" }
{ "index" : { "_id" : "6" } }
{ "foo" : "spam" }
#####################

GET /foo/bar/_search
{
    "size": 0,
    "aggs": {
        "the_foos": {
            "terms": {
                "field": "foo",
                "size": 10
            }
        }
    }
}
#####################
PUT _watcher/watch/transform
{
    "input": {
        "search": {
            "request": {
                "indices": [
                    "foo"
                ],
                "types": [
                    "bar"
                ],
                "body": {
                    "size": 0,
                    "aggs": {
                        "the_foos": {
                            "terms": {
                                "field": "foo",
                                "size": 10
                            }
                        }
                    }
                }
            }
        }
    },
    "trigger": {
        "schedule": {
            "interval": "1h"
        }
    },
    "actions": {
        "index_payload": {
            "transform": {
                "script": "return [ _doc : ctx.payload.aggregations.the_foos.buckets ]"
            },
            "index": {
                "index": "my-index",
                "doc_type": "my-type"
            }
        }
    }
}
#####################
POST _watcher/watch/transform/_execute
#####################
GET my-index/my-type/_search
#####################
#####################
#####################

resp = requests.post('http://localhost:9200/activities_day_stat/_search?size=0',json=query)
if resp.status_code != 200:
    print("resp=",resp.content)
    raise Exception('POST {}'.format(resp.status_code))

pprint.pprint(resp.json())

