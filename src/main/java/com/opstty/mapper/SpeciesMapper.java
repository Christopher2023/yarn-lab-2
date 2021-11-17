package com.opstty.mapper;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;



public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String DELIMITER = ";";
        String line = value.toString();
        String[] list = line.split(DELIMITER);
        word.set(list[3]);
        context.write(word,NullWritable.get());
    }
}
