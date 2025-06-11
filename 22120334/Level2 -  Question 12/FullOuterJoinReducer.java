package org.example.q12;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FullOuterJoinReducer extends Reducer<Text, Text, Text, Text> {
  private Text resultValue = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    String price = "null";
    String quantity = "null";
    boolean priceFound = false;
    boolean quantityFound = false;


    for (Text val : values) {
      String taggedValue = val.toString();
      if (taggedValue.startsWith("P:")) {
        price = taggedValue.substring(2);
        priceFound = true;
      } else if (taggedValue.startsWith("Q:")) {
        quantity = taggedValue.substring(2);
        quantityFound = true;
      }
    }

    if (priceFound || quantityFound) {
      resultValue.set(price + "\t" + quantity);
      context.write(key, resultValue);
    }
  }
}
