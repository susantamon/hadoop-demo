package com.here.example;

import com.here.example.WordCount.Map;
import com.here.example.WordCount.Reduce;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordCountTest {

  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {
    mapDriver = MapDriver.newMapDriver(new Map());
    reduceDriver = ReduceDriver.newReduceDriver(new Reduce());
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(new Map(), new Reduce());
  }

  @Test
  public void testWordCountMapper() throws IOException {
    mapDriver.withInput(new LongWritable(), new Text("hi there hi"))
        .withOutput(new Text("hi"), new IntWritable(1))
        .withOutput(new Text("there"), new IntWritable(1))
        .withOutput(new Text("hi"), new IntWritable(1))
        .runTest();
  }

  @Test
  public void testWordCountReducer() throws IOException {
    reduceDriver.withInput(new Text("hi"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
        .withOutput(new Text("hi"), new IntWritable(2))
        .runTest();
  }

  @Test
  public void testWordCountMapperReducer() throws IOException {
    mapReduceDriver.withInput(new LongWritable(), new Text("hi there hi"))
        .withOutput(new Text("hi"), new IntWritable(2))
        .withOutput(new Text("there"), new IntWritable(1))
        .runTest();
  }

}
