package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SortMapperTest {
    @Mock
    private Mapper.Context context;
    private SortMapper sortMapper;

    @Before
    public void setup() {
        this.sortMapper = new SortMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        String v = "arbre";
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars\n";
        this.sortMapper.map(null, new Text(value), this.context);
        verify(this.context, times(1))
                .write(new Text("arbre"), new Text("13.0"));
    }
}