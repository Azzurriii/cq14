package org.example.q13;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class LkMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text itemsetText = new Text();
  private int k;
  private List<List<String>> Lk_1_itemsets = new ArrayList<>();
  private Set<String> Lk_1_stringSet = new HashSet<>();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    k = conf.getInt("k.value", 2);

    URI[] cacheFiles = context.getCacheFiles();
    if (cacheFiles != null && cacheFiles.length > 0) {
      for (URI cacheFile : cacheFiles) {
        Path Lk_1_Path = new Path(cacheFile.getPath());
        String Lk_1_FileName = Lk_1_Path.getName().toString();
        try (BufferedReader reader = new BufferedReader(new FileReader(Lk_1_FileName))) {
          String line;
          while ((line = reader.readLine()) != null) {
            String itemsetStr = line.split("\t")[0];
            Lk_1_itemsets.add(Utils.stringToItemset(itemsetStr));
            Lk_1_stringSet.add(itemsetStr);
          }
        }
      }
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\t");
    if (parts.length < 2) return;

    String itemsString = parts[1];
    StringTokenizer tokenizer = new StringTokenizer(itemsString);
    List<String> transactionItems = new ArrayList<>();
    while (tokenizer.hasMoreTokens()) {
      transactionItems.add(tokenizer.nextToken());
    }
    Collections.sort(transactionItems);

    List<List<String>> candidates = generateCandidatesFromTransaction(transactionItems, k);

    for (List<String> candidate : candidates) {
      if (k > 1 && !allSubsetsFrequent(candidate, k - 1, Lk_1_stringSet)) {
        continue;
      }
      itemsetText.set(Utils.itemsetToString(candidate));
      context.write(itemsetText, one);
    }
  }

  private List<List<String>> generateCandidatesFromTransaction(List<String> transactionItems, int candidateSize) {
    return Utils.generateSubsets(transactionItems, candidateSize);
  }

  private boolean allSubsetsFrequent(List<String> candidate, int subsetSize, Set<String> frequentPrevLevel) {
    if (candidate.size() < subsetSize || subsetSize <=0) return true;
    List<List<String>> subsets = Utils.generateSubsets(candidate, subsetSize);
    for (List<String> subset : subsets) {
      if (!frequentPrevLevel.contains(Utils.itemsetToString(subset))) {
        return false;
      }
    }
    return true;
  }
}