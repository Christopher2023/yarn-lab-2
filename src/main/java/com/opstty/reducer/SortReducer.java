package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SortReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int i;
        List<Double> list = new ArrayList<Double>();
        for (Text val : values) {
            list.add(Double.parseDouble(String.valueOf(val)));
        }
        Collections.sort(list);
        String r = list.stream()
                .map(n -> String.valueOf(n))
                .collect(Collectors.joining(" - ", " { ", " } "));
        result.set(r);
        context.write(key, result);
    }
}
