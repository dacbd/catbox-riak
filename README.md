[![Build Status](https://travis-ci.org/DanielBarnes/catbox-riak.png?branch=master)](https://travis-ci.org/DanielBarnes/catbox-riak)


catbox-riak
===========

Riak adapter [for catbox](https://github.com/spumko/catbox)

### Options

- `host` - the Riak server hostname. Defaults to `127.0.0.1`.
- `port` - the Riak PBC port. Defaults to `8087`.
- `partition` - the partition will choose what riak bucket your cache will be stored in.
- `ttl_interval` - the interval at which the the riak GC function will run in milliseconds( < `2147483647` or about 596 hours), OR `false` to bypass running the GC function.


### Notes

Since Riak doesn't have ttl built in, a garbage collection function will run periodically to remove expired keys. This function makes a getIndex call, so your riak backend cannot be set to `riak_kv_bitcask_backend`, this call streams the keys that need to be deleted and deletes them as they are received. 
In order to prevent siblings we recomend you set `last_write_wins` on the bucket to true.
The `ttl_interval` field is required to be your interval in milliseconds (making your effective ttl for items in your cache a multiple of your `ttl_interval`) OR set to `false` which will cause this instance of your cache to never run the GC function, if this is the only server using that cache then no keys will ever expire.
