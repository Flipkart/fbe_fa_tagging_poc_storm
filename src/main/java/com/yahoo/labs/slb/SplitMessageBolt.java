package com.yahoo.labs.slb;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class SplitMessageBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    Map<String, String> ratioMap;
    Map<String, Integer> idempotenceMap;
    Map<String, Integer> testMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        RatioMap.setDefaultValue();
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
        this.idempotenceMap = new HashMap<String, Integer>();
        this.ratioMap = new LinkedHashMap<>();
        this.testMap = new LinkedHashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        ratioMap = RatioMap.getRatioMap();
        String warehouseId = tuple.getString(0);
        String wid = tuple.getString(1);
        String sellerId = tuple.getString(2);
        Integer quantity = tuple.getInteger(3);
        Integer idempotenceKey = tuple.getInteger(4);
        String key = warehouseId + "_" + wid + "_" + sellerId;

        if (!counterMap.containsKey(key) || !idempotenceMap.containsKey(key)) {
            counterMap.put(key, 1);
            idempotenceMap.put(key, idempotenceKey);
            testMap.put(key, 0);
            try {
                splitInput(key, quantity, idempotenceKey);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            if(idempotenceKey > idempotenceMap.get(key)){
                idempotenceMap.put(key, idempotenceKey);
                counterMap.put(key, counterMap.get(key) + 1);
                try {
                    splitInput(key, quantity, idempotenceKey);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                return;
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void splitInput(String key, Integer quantity, Integer idempotenceKey) throws Exception {
        int counter = counterMap.get(key);

        JSONParser parser = new JSONParser();
        JSONObject ratioJson = (JSONObject) parser.parse(ratioMap.get(key));
        int sum = 0;

        Set keySet = ratioJson.keySet();
        for (Object value : keySet) {
            sum += ((Long)ratioJson.get(value)).intValue();
        }

        int factor = quantity / sum;
        int mod = quantity % sum;

        int shareTill = 0;
        int roundCounter = 0;
        int roundMod, roundRun;
        roundMod = roundRun = mod - (sum - counter);

        ArrayList list = new ArrayList();
        for (Object salesChannel : keySet) {
            if (counter >= sum)
                counter = counter % sum;
            int share = ((Long)ratioJson.get(salesChannel)).intValue();;
            shareTill += share;
            int remainingShare = shareTill - counter;
            int qcount = share * factor;
            if (mod > 0 && remainingShare > 0) {
                if (mod >= remainingShare) {
                    qcount += remainingShare;
                    counter += remainingShare;
                    mod -= remainingShare;
                } else {
                    qcount += mod;
                    counter += mod;
                    mod = 0;
                }
            } else if (remainingShare < 0 && roundRun > 0) {
                if (roundRun >= share) {
                    qcount += share;
                    roundCounter += share;
                    roundRun -= share;
                } else {
                    qcount += roundRun;
                    roundCounter += roundRun;
                    roundRun = 0;
                }
            }

            HashMap result = new HashMap();
            result.put("count", qcount);
            result.put("salesChannel", salesChannel);
            String [] data = key.split("_");
            result.put("warehouseId", data[0]);
            result.put("listingId", data[1]);
            result.put("sellerId", data[2]);
            result.put("idempotenceKey", idempotenceKey);

            list.add(result);
            if (roundMod > 0)
                counterMap.put(key, roundMod);
            else
                counterMap.put(key, counter);
        }

        HashMap request = new HashMap();
        request.put("bucketList", list);
        System.out.println("******************************" + request);
//        callBucket(JSON.toString(request));

//        int quantity2 = testMap.get(key) + quantity;
//        testMap.put(key, quantity2);
//
//        Thread.sleep(100);
//
//        System.out.println("******************************" +  testMap);
    }


    private static void callBucket(String payload) {
        try {

            URL url = new URL("http://172.17.94.185:8080/api/bulkcreate");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("PUT");
            conn.setRequestProperty("Content-Type", "application/json");

            OutputStream os = conn.getOutputStream();
            os.write(payload.getBytes());
            os.flush();

            if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }

            conn.disconnect();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

