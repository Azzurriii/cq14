package org.example.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TransactionCostMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private Text outKey = new Text();
  private DoubleWritable outValue = new DoubleWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\t");
    if (parts.length == 2) {
      String transactionId = parts[0];
      String itemString = parts[1];

      String itemName = itemString;
      boolean isDiscounted = false;

      if (itemString.endsWith("*")) {
        itemName = itemString.substring(0, itemString.length() - 1);
        isDiscounted = true;
      }

      int baseCost = itemName.replaceAll("\\s+", "").length();
      double actualCost = isDiscounted ? baseCost * 0.8 : baseCost;

      outKey.set(transactionId);
      outValue.set(actualCost);
      context.write(outKey, outValue);
    }
  }
}