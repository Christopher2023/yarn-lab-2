package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.*;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortReducerTest {
    @Mock
    private Reducer.Context context;
    private SortReducer sortReducer;

    @Before
    public void setup() {
        this.sortReducer = new SortReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        Text value = new Text("100");
        Text value2 = new Text("12.9");
        Text value3 = new Text("19.0");
        Text value4 = new Text("19.1");
        Iterable<Text> values = Arrays.asList(value, value2, value3, value4);
        this.sortReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new Text(" { 12.9 - 19.0 - 19.1 - 100.0 } "));
    }
}
