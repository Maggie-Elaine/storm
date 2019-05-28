# storm
基于sotrm的缓存预热与热点缓存监控

1.可以用nginx+lua分发服务,把message推送到kafka

2.storm从kafka中拉取信息,spout+bolt处理message,用LRUMap存放筛选后的热点信息

3.把热点信息存放到zookeeper或者redis中,写一后台服务从缓存服务器总获取信息,并加入到redis或者本地缓存
