package com.hadoop.amazon;

import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.io.Writable;
import java.util.Set;

class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();
        for (Object key : keySet) {
            String[] keys = key.toString().split(":");
            result.append(keys[0] + ":" + keys[1]+this.get(key));
        }
        return result.toString();
    }
}
