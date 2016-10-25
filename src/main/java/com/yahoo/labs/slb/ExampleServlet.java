package com.yahoo.labs.slb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.eclipse.jetty.http.HttpStatus;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ExampleServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new LogReaderSpout());
        builder.setBolt("call-log-counter-bolt", new SplitMessageBolt())
                .fieldsGrouping("call-log-reader-spout", new Fields("warehouseId", "wid"));
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();

        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println("Started");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String key = req.getParameter("key");
        String ratioValue = req.getParameter("ratioValue");
        RatioMap.setRatioMap(key, ratioValue);

        resp.setStatus(HttpStatus.OK_200);
        resp.getWriter().println("Ratio Value Updated: " + RatioMap.getRatioMap().get(key));
    }

}