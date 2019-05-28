package com.beneil.storm.monitor;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 热数据统计拓扑
 */
public class HotProductTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("AccessLogKafkaSpout",new AccessLogKafkaSpout(),1);
        builder.setBolt("LogParseBolt",new LogParseBolt(),3)
                .setNumTasks(3)
                .shuffleGrouping("AccessLogKafkaSpout");
        builder.setBolt("ProductCountBolt",new ProductCountBolt(),5)
                .setNumTasks(5)
                .fieldsGrouping("LogParseBolt",new Fields("productId"));


        Config config = new Config();
        if(args!=null&&args.length>0){
            config.setNumWorkers(3);
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
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology",config,builder.createTopology());
            Utils.sleep(30000);
            cluster.shutdown();
        }

    }
}
