package com.beneil.storm.monitor;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class AccessLogKafkaSpout  extends BaseRichSpout {
    /**
     * kafka消费spout
     */
    private  ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);
    private SpoutOutputCollector collector;
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
        startKafkaConsumer();
    }
    //-----------------------------------------------------------------------------------//
    private void startKafkaConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","192.168.240.129:2181,192.168.240.129:2182,192.168.240.129:2183");
        properties.put("group.id","cache-group");
        properties.put("zookeeper.session.timeout.ms","40000");
        properties.put("zookeeper.sync.time.ms","200");
        properties.put("auto.commit.interval.ms","1000");

        ConsumerConfig config = new ConsumerConfig(properties);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);

        String topic ="access-log";
        Map<String,Integer> topicCountMap=new HashMap<String, Integer>();
        topicCountMap.put(topic,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            new Thread(new KafkaMessageProcessor(stream)).start();//从kafka中拿数据 放入queue中
        }
    }

    private class KafkaMessageProcessor implements Runnable {

        @SuppressWarnings("rawtypes")
        private KafkaStream kafkaStream;

        @SuppressWarnings("rawtypes")
        public KafkaMessageProcessor(KafkaStream kafkaStream) {
            this.kafkaStream = kafkaStream;
        }

        @SuppressWarnings("unchecked")
        public void run() {
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
            while (it.hasNext()) {
                String message = new String(it.next().message());//不断拿数据
                try {
                    queue.put(message);//存放
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    //-----------------------------------------------------------------------------------//

    public void nextTuple() {//发送tuple
        if(queue.size()>0){
            try {
                String message = queue.take();
                collector.emit(new Values(message));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            Utils.sleep(100);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }



}
