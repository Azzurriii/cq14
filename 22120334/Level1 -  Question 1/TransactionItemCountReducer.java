package org.example.q1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionItemCountReducer extends Reducer<Text, Text, Text, Text> {
  private Text result = new Text();

  private static class ItemCount implements Comparable<ItemCount> {
    String item;
    int count;

    ItemCount(String item, int count) {
      this.item = item;
      this.count = count;
    }

    @Override
    public int compareTo(ItemCount other) {
      if (this.count != other.count) {
        return Integer.compare(other.count, this.count); // Sort by count descending
      }
      return this.item.compareTo(other.item); // Then by item name ascending
    }

    @Override
    public String toString() {
      return "[" + item + ", " + count + "]";
    }
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Map<String, Integer> itemCountsMap = new HashMap<>();
    for (Text val : values) {
      String itemName = val.toString();
      itemCountsMap.put(itemName, itemCountsMap.getOrDefault(itemName, 0) + 1);
    }

    List<ItemCount> sortedItems = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : itemCountsMap.entrySet()) {
      sortedItems.add(new ItemCount(entry.getKey(), entry.getValue()));
    }
    Collections.sort(sortedItems);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < sortedItems.size(); i++) {
      sb.append(sortedItems.get(i).toString());
      if (i < sortedItems.size() - 1) {
        sb.append(" ");
      }
    }
    result.set(sb.toString());
    context.write(key, result);
  }
}