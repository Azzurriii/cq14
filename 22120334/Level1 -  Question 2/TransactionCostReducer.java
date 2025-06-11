package org.example.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat;

public class TransactionCostReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private DoubleWritable result = new DoubleWritable();

  @Override
  protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0;
    for (DoubleWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}