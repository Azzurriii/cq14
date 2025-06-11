package org.example.q13;

import java.util.*;

public class Utils {

  public static String itemsetToString(List<String> itemset) {
    Collections.sort(itemset);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < itemset.size(); i++) {
      sb.append(itemset.get(i));
      if (i < itemset.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static List<String> stringToItemset(String s) {
    return new ArrayList<>(Arrays.asList(s.split(",")));
  }

  public static List<List<String>> generateSubsets(List<String> itemset, int k) {
    List<List<String>> subsets = new ArrayList<>();
    int n = itemset.size();
    if (k < 0 || k > n) {
      return subsets;
    }
    int[] s = new int[k];
    if (k == 0) {
      subsets.add(new ArrayList<>());
      return subsets;
    }
    for (int i = 0; (s[i] = i) < k - 1; i++);
    subsets.add(getSublist(itemset, s));
    for (;;) {
      int i;
      for (i = k - 1; i >= 0 && s[i] == n - k + i; i--);
      if (i < 0) {
        break;
      }
      s[i]++;
      for (++i; i < k; i++) {
        s[i] = s[i - 1] + 1;
      }
      subsets.add(getSublist(itemset, s));
    }
    return subsets;
  }

  private static List<String> getSublist(List<String> itemset, int[] subsetIndices) {
    List<String> sub = new ArrayList<>();
    for (int index : subsetIndices) {
      sub.add(itemset.get(index));
    }
    return sub;
  }
}
