package com.yahoo.labs.slb;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {
    public static void main(String[] args) throws Exception{
        //Create Config instance for cluster configuration
        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new LogReaderSpout());
        builder.setBolt("call-log-counter-bolt", new SplitMessageBolt())
                .fieldsGrouping("call-log-reader-spout", new Fields("warehouseId", "wid"));

        StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
//        Thread.sleep(10000);
//        cluster.shutdown();
    }
}

