package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        List<Double> list = new  ArrayList<Double>();
        for (DoubleWritable val : values) {
            list.add(val.get());
        }
        Double max = Collections.max(list);
        result.set(max);
        context.write(key, result);
    }
}
