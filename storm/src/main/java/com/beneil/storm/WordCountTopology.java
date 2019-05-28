package com.beneil.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * 单词计数
 * //这个用于测试
 */
public class WordCountTopology {
    /**
     * spout
     * 继承一个基类 实现接口 主要是负责 从数据源获取数据
     */
    public static class RandomSentenceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;
        private Random random;

        /**
         *open对 spout初始化 比如说创建线程池 或者创建一个数据库连接池 或者构建一个httpclient
         */
        public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
            this.collector=collector;//用来发送数据
            this.random=new Random();//构造随机数对象
        }

        /**
         * 这个spout类 最终运行在task中(某个worker进程的某个executor线程内部的某个task)
         * task无线循环  调用nextTuple  可以不断发送数据 形成数据流
         */
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
                    "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};
            String sentence=sentences[random.nextInt(sentences.length)];
            System.out.println("发送句子");
            collector.emit(new Values(sentence));//values  可以认为构建一个tuple(最小的数据单位)  -->组成stream

        }

        /**
         * 定义一个你发送出去的每个tuple中的每个field 的 名称
         *
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }
    //==========================================================================================//
    /**
     * bolt
     * 继承基类 每个bolt 同样worker--->executor--->task
     */
    public static class SplitSentence extends BaseRichBolt{
        private  OutputCollector collector;

        /**
         * OutputCollector也是这个bolt的tuple发射器
         */
        public void prepare(Map map, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        /**
         * 每次接受到数据后 会通过executor执行
         */
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words=sentence.split(" ");
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        /**
         * 定义发射的tuple 的filed名称
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
    //==========================================================================================//
    public static class WordCount extends BaseRichBolt{
        private  OutputCollector collector;
        private Map<String,Long> wordCounts=new HashMap<String, Long>();

        public void prepare(Map map, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            Long count = wordCounts.get(word);
            if(count==null){
                count=0L;
            }
            count++;
            wordCounts.put(word,count);
            System.out.println("统计单词"+word+" : "+count);
            collector.emit(new Values(word,count));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
        }
    }
    //==========================================================================================//
    public static void main(String[] args) {
        //将spout和bolt组合起来  构建成一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomSentence",new RandomSentenceSpout(),2);//名称/创建spout对象/创建executor个数
        builder.setBolt("SplitSentence",new SplitSentence(),5)
                .setNumTasks(10)//不设置task 默认一个
                .shuffleGrouping("RandomSentence");//设置task数量/对RandomSentence 随机发送
        builder.setBolt("WordCount",new WordCount(),10)
                .setNumTasks(20)
                .fieldsGrouping("SplitSentence",new Fields("word"));//很重要,相同的单词 从splitSentence发射出来 一定会进入相同的task中 才能统计数目

        Config config = new Config();
        //说明在命令行执行  打算提交到storm集群上
        if(args!=null&&args.length>0){
            config.setNumWorkers(3);//设置worker
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            //说明在ide本地运行
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCountTopology",config,builder.createTopology());
            Utils.sleep(5000);//运行10s
            cluster.shutdown();
        }

    }


}
