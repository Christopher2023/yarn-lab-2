/*package com.opstty.mapper;


import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class SortMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private DoubleWritable one = new DoubleWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String DELIMITER = ";";
        String line = value.toString();
        String[] list = line.split(DELIMITER);
        try {
            one.set(Double.parseDouble(list[6]));
        }catch (Exception e){ }
        word.set(list[2]);

        context.write(word, one);
    }
}*/
package com.opstty.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, Text, Text> {
    private Text one = new Text();
    private Text word = new Text("arbre");

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String DELIMITER = ";";
        String line = value.toString();
        String[] list = line.split(DELIMITER);
        try {
            one.set(list[6]);
        }catch (Exception e){ }


        context.write(word, one);
    }
}
