# Elasticsearch Dumper

## EXAMPLE:
```elasticsearch-dumper -s http://source:9200 -d http://destination:9200 -i index1,index2```

## INSTALL:
1. ```go get github.com/hoffoo/elasticsearch-dump```
2. or download a prebuilt binary here: https://github.com/hoffoo/elasticsearch-dump/releases/


```
Application Options:
  -s, --source=     source elasticsearch instance
  -d, --dest=       destination elasticsearch instance
  -c, --count=      number of documents at a time: ie "size" in the scroll request (100)
  -t, --time=       scroll time (1m)
  -f, --force       delete destination index before copying (false)
      --shards=     set a number of shards on newly created indexes
      --docs-only   load documents only, do not try to recreate indexes (false)
      --index-only  only create indexes, do not load documents (false)
      --replicate   enable replication while indexing into the new indexes (false)
  -i, --indexes=    list of indexes to copy, comma separated (_all)
  -a, --all         copy indexes starting with . and _ (false)
  -w, --workers=    concurrency (1)
      --settings    copy sharding settings from source (true)
      --green       wait for both hosts cluster status to be green before dump. otherwise yellow is okay (false)
```


## NOTES:

1. Has been tested getting data from 0.9 onto a 1.4 box. For other scenaries YMMV. (look out for this bug: https://github.com/elasticsearch/elasticsearch/issues/5165)
1. Copies using the [_source](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-source-field.html) field in elasticsearch. If you have made modifications to it (excluding fields, etc) they will not be indexed on the destination host.
1. ```--force``` will delete indexes on the destination host. Otherwise an error will be returned if the index exists
1. ```--time``` is the [scroll time](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context) passed to the source host, default is 1m. This is a string in es's format.
1. ```--count``` is the [number of documents](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-scan) that will be request and bulk indexed at a time. Note that this depends on the number of shards (ie: size of 10 on 5 shards is 50 documents)
1. ```--indexes``` is a comma separated list of indexes to copy
1. ```--all``` indexes starting with . and _ are ignored by default, --all overrides this behavior
1. ```--workers``` concurrency when we post to the bulk api. Only one post happens at a time, but higher concurrency should give you more throughput when using larger scroll sizes.
1. Ports are required, otherwise 80 is the assumed port (what)

## BUGS:

1. It will not do anything special when copying the _id (copies _id from source host). If _id is remapped it may not do what you want.
1. Should assume a default port of 9200
