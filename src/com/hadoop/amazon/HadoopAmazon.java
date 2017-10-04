package com.hadoop.amazon;

import java.io.IOException;
import java.util.*;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopAmazon {
    public static List<String> sortMapByValue(HashMap<String, Integer> map) {
        int size = map.size();
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(size);
        list.addAll(map.entrySet());
        ValueComparator vc = new ValueComparator();
        Collections.sort(list, vc);
        final List<String> keys = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            keys.add(i, list.get(i).getKey());
        }
        return keys;
    }

    private static class ValueComparator implements Comparator<Map.Entry<String, Integer>> {
        public int compare(Map.Entry<String, Integer> mp1, Map.Entry<String, Integer> mp2) {
            return mp1.getValue() - mp2.getValue();
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text,MyMapWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
              String[] items = value.toString().split(" ");
                for (int i = 0; i < items.length; i++) {
                    MyMapWritable map = new MyMapWritable();
                    for (int j = 0; j < items.length; j++) {
                        if (j != i) {
                            map.put(new Text(items[j]), one);
                        }
                    }
                    context.write(new Text(items[i]),map);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, MyMapWritable, Text,MyMapWritable2> {
        public void reduce(Text key, Iterable<MyMapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String, Integer> hm = new HashMap<String, Integer>();
            for (MyMapWritable val : values) {
                for (Writable k : val.keySet()) {
                    if(hm.isEmpty()){
                        hm.put(k.toString(),1);
                    }else if(hm.containsKey(k.toString())){
                        int ii = (hm.get(k.toString()));
                        ii++;
                        hm.put(k.toString() ,ii);
                    }else{
                        hm.put(k.toString(),1);
                    }
                }
            }
            List<String> l = HadoopAmazon.sortMapByValue(hm);
            MyMapWritable2 mapWritable = new MyMapWritable2();

            for(int i =0;i<l.size();i++){
                System.out.println(i+l.get(i));
                mapWritable.put(new IntWritable(hm.get((l.get(i)))),new Text(l.get(i)));
            }
            context.write(key,mapWritable);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amazon");
        job.setJarByClass(HadoopAmazon.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMapWritable2.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}