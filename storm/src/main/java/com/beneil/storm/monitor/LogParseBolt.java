package com.beneil.storm.monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.support.hsf.HSFJSONUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogParseBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    public void execute(Tuple tuple) {
        String message = tuple.getStringByField("message");
        JSONObject jsonObject = JSON.parseObject(message);
        JSONObject uriArgs = jsonObject.getJSONObject("uri_args");
        Long productId = uriArgs.getLong("productId");
        if(productId!=null){
            collector.emit(new Values(productId));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("productId"));
    }
}
