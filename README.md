catbox-riak
===========

Riak adapter for catbox

### Options

- `host` - the Riak server hostname. Defaults to `127.0.0.1`.
- `port` - the Riak PBC port. Defaults to `8087`.


### Notes

Since Riak doesn't have ttl built in, a garbage collection function will run periodically to remove expired keys. This function makes a getIndex call, so your riak backend cannot be set to `riak_kv_bitcask_backend`, this call streams the keys that need to be deleted and deletes them as they are received. 
In order to prevent siblings we recomend you set `last_write_wins` on the bucket to true.
