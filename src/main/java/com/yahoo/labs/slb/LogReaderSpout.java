package com.yahoo.labs.slb;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

//import storm tuple packages
//import Spout interface packages

//Create a class LogReaderSpout which implement IRichSpout interfaceto access functionalities

public class LogReaderSpout implements IRichSpout {
    //Create instance for SpoutOutputCollector which passes tuples to bolt.
    private SpoutOutputCollector collector;
    private boolean completed = false;

    //Create instance for TopologyContext which contains topology data.
    private TopologyContext context;

    //Create instance for Random class.
    private Random randomGenerator = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        generateStream();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("warehouseId", "wid", "sellerId", "quantity", "idempotenceKey"));
    }

    //Override all the interface methods
    @Override
    public void close() {}

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void generateStream(){
        Integer localIdx = 0;
        while(localIdx++ < 100) {
//            String listingId = "WID" + randomGenerator.nextInt(2);
//            String warehouseId = "warehouse" + randomGenerator.nextInt(2);
//            String sellerId = "sellerId" + randomGenerator.nextInt(2);

            String listingId = "WID0";
            String warehouseId = "warehouse0";
            String sellerId = "sellerId0";


            Integer quantity = randomGenerator.nextInt(100);
            Integer idempotenceKey = randomGenerator.nextInt(1000);
            this.collector.emit(new Values(warehouseId, listingId, sellerId, quantity, idempotenceKey));
        }
    }
}
