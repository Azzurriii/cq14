package org.example.q1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionItemCountMapper extends Mapper< LongWritable, Text, Text, Text> {
  private Text outKey = new Text();
  private Text outValue = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\t");
    if (parts.length == 2) {
      outKey.set(parts[0]); // transaction_id
      outValue.set(parts[1]); // item_name
      context.write(outKey, outValue);
    }
  }
}