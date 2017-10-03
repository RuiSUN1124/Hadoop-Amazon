package com.hadoop.amazon;

import java.io.IOException;
import java.util.HashMap;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopAmazon {
    public static String itemBought = "begin";
    public static HashMap<String, Integer> itemRest = new HashMap<String, Integer>();

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
            return mp2.getValue() - mp1.getValue();
        }
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] values = value.toString().split("\n");
            for (String v : values) {
                String[] items = v.toString().split(" ");
                for (int i = 0; i < items.length; i++) {
                    // HashMap<String,String> hm = new HashMap<String,String>();
                    for (int j = 0; j < items.length; j++) {
                        if (j != i) {
                            MyMapWritable map = new MyMapWritable();
                            map.put(new Text(items[i] + ":" + items[j]), new Text(""));
                            word.set(map.toString());
                            context.write(word, one);
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            String key_string = new String(key.toString());

            if (key_string.contains(":")) {
                String k1 = new String((key_string.split(":"))[0]);
                String k2 = new String((key_string.split(":"))[1]);
                if (k1.equals(itemBought)) {
                    itemRest.put(k2, sum);
                } else {
                    String tmp = new String(itemBought);
                    HashMap<String, Integer> hmtmp = new HashMap<String, Integer>(itemRest);
                    itemRest.clear();
                    itemBought = k1;
                    itemRest.put(k2, sum);
                    if (!tmp.equals("begin"))
                        context.write(new Text(tmp + " " + HadoopAmazon.sortMapByValue(hmtmp).toString()), new IntWritable(Collections.max(hmtmp.values())));
                }
            } else {
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(HadoopAmazon.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}