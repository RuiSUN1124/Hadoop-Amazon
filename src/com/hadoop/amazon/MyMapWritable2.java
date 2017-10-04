package com.hadoop.amazon;

import org.apache.hadoop.io.MapWritable;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Set;

class MyMapWritable2 extends SortedMapWritable {

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        Set<WritableComparable> keySet = this.keySet();
        for (Object key : keySet) {
            result.insert(0,this.get(key)+":"+key+" ");
        }
        return result.toString();
    }
}
