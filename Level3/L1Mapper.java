package org.example.q13;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class L1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text itemText = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\t");
    if (parts.length == 2) {
      String itemsString = parts[1];
      StringTokenizer tokenizer = new StringTokenizer(itemsString);
      while (tokenizer.hasMoreTokens()) {
        itemText.set(tokenizer.nextToken());
        context.write(itemText, one);
      }
    }
  }
}