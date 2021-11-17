package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;


import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class HeightReducerTest {
    @Mock
    private Reducer.Context context;
    private HeightReducer heightReducer;

    @Before
    public void setup() {
        this.heightReducer = new HeightReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "key";
        DoubleWritable value = new DoubleWritable(1.0);
        DoubleWritable value1 = new DoubleWritable(5.0);
        DoubleWritable value2 = new DoubleWritable(9.0);
        DoubleWritable value3 = new DoubleWritable(13.0);
        Iterable<DoubleWritable> values = Arrays.asList(value, value1, value2, value3);
        this.heightReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), new DoubleWritable(13.0));
    }
}
