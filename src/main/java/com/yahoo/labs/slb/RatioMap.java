package com.yahoo.labs.slb;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by anurag.gupta on 24/10/16.
 */
public class RatioMap {
    static Map<String, String> ratioMap = new LinkedHashMap<>();

    static Map<String, String> getRatioMap(){
        return ratioMap;
    }

    static void setRatioMap(String key, String value){
        ratioMap.put(key, value);
    }

    static void setDefaultValue(){
        setRatioMap("warehouse0_WID0_sellerId0", "{\"p1\":1,\"p2\":2,\"p3\":3}");
        setRatioMap("warehouse0_WID0_sellerId1", "{\"p1\":1,\"p2\":2,\"p3\":3}");
        setRatioMap("warehouse0_WID1_sellerId0", "{\"p1\":2,\"p2\":5,\"p3\":5}");
        setRatioMap("warehouse0_WID1_sellerId1", "{\"p1\":2,\"p2\":1,\"p3\":5}");
        setRatioMap("warehouse1_WID0_sellerId0", "{\"p1\":1,\"p2\":2,\"p3\":3}");
        setRatioMap("warehouse1_WID0_sellerId1", "{\"p1\":1,\"p2\":7,\"p3\":3}");
        setRatioMap("warehouse1_WID1_sellerId0", "{\"p1\":2,\"p2\":5,\"p3\":5}");
        setRatioMap("warehouse1_WID1_sellerId1", "{\"p1\":4,\"p2\":1,\"p3\":5}");
    }
}
