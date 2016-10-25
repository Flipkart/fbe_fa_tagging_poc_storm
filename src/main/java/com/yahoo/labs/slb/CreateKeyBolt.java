package com.yahoo.labs.slb;
//import util packages
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

//import Storm IRichBolt package

//Create a class CreateKeyBolt which implement IRichBolt interface
public class CreateKeyBolt implements IRichBolt {
    //Create instance for OutputCollector which collects and emits tuples to produce output
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String warehouseId = tuple.getString(0);
        String wid = tuple.getString(1);
        Integer quantity = tuple.getInteger(2);
        collector.emit(new Values(warehouseId + "_" + wid, quantity));
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call", "quantity"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
