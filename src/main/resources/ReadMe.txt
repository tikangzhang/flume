# Flume拦截器配置举例
...

a1.sources.r1.interceptors =  i1 i2
a1.sources.r1.interceptors.i1.type = com.laozhang.flume.interceptor.FilterInterceptor$Builder
a1.sources.r1.interceptors.i2.type = com.laozhang.flume.interceptor.DistributeInterceptor$Builder

a1.sources.r1.selector.type = multiplexing
a1.sources.r1.selector.header = topic
a1.sources.r1.selector.mapping.topic_load = c1
a1.sources.r1.selector.mapping.topic_prod = c2

a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = saas01:9092,saas02:9092,saas03:9092
a1.channels.c1.kafka.topic = topic_load
a1.channels.c1.parseAsFlumeEvent = false
a1.channels.c1.kafka.consumer.group.id = flume-consumer

a1.channels.c2.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c2.kafka.bootstrap.servers = saas01:9092,saas02:9092,saas03:9092
a1.channels.c2.kafka.topic = topic_prod
a1.channels.c2.parseAsFlumeEvent = false
a1.channels.c2.kafka.consumer.group.id = flume-consumer
