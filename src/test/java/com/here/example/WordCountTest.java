package com.here.example;

import com.here.example.WordCount.Map;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

  @Before
  public void setUp() {
    mapDriver = MapDriver.newMapDriver(new Map());
  }

  @Test
  public void testWordCountMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text("hi"))
        .withOutput(new Text("hi"), new IntWritable(1))
        .runTest();
  }
}
