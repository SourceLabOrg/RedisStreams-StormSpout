# Change Log
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## 1.1.0 (07/24/2020)
- Add Jedis implementation.  Spout defaults to using the Lettuce redis library, but you can configure
  to use the Jedis library instead via the `RedisStreamSpoutConfig.withJedisClientLibrary()` method.
- Bugfix on Spout deploy, consumer thread started during `open()` lifecycle call instead of `activate()`. 
- Bugfix on Spout restart, resume consuming first from consumers personal pending list.

## 1.0.0 (07/20/2020)
- Initial release!
