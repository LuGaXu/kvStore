package nju.bigdata.model;

import java.io.Serializable;
import java.util.Map;

public class Item implements Serializable{

    private String key;
    private Map<String, String> value;

    public String getKey() {
        return key;
    }

    public Map<String, String> getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(Map<String, String> value) {
        this.value = value;
    }
}
