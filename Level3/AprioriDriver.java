package org.example.q13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ToolRunner; // Thêm nếu bạn muốn dùng Tool
import org.apache.hadoop.conf.Configured;    // Thêm nếu bạn muốn dùng Tool
import org.apache.hadoop.util.Tool;         // Thêm nếu bạn muốn dùng Tool


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

// public class AprioriDriver { // Bỏ static nếu dùng Tool
public class AprioriDriver extends Configured implements Tool { // Triển khai Tool

  // public static void main(String[] args) throws Exception { // Chuyển thành run
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: AprioriDriver <inputPath> <outputPathBase> <minSupportDecimal>");
      // System.exit(-1);
      return -1;
    }

    String inputPath = args[0];
    String outputPathBase = args[1];
    double minSupportDecimal = Double.parseDouble(args[2]);

    // Configuration conf = new Configuration(); // Lấy từ getConf() khi dùng Tool
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    long totalTransactions = 0;
    Path hdfsInputPath = new Path(inputPath);
    if (!fs.exists(hdfsInputPath) || !fs.isDirectory(hdfsInputPath)) {
      System.err.println("Input path does not exist or is not a directory: " + inputPath);
      return 1;
    }
    FileStatus[] status = fs.listStatus(hdfsInputPath);
    for (FileStatus stat : status) {
      if (!stat.isDirectory() && !stat.getPath().getName().startsWith("_")) { // Bỏ qua các file ẩn
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())))) {
          while (reader.readLine() != null) {
            totalTransactions++;
          }
        }
      }
    }
    if (totalTransactions == 0) {
      System.err.println("No transactions found in input path: " + inputPath);
      // System.exit(1);
      return 1;
    }

    int minSupportCount = (int) Math.ceil(minSupportDecimal * totalTransactions);
    if (minSupportCount == 0 && totalTransactions > 0) minSupportCount = 1;
    conf.setInt("minSupportCount", minSupportCount);

    System.out.println("Total Transactions: " + totalTransactions);
    System.out.println("Minimum Support Decimal: " + minSupportDecimal);
    System.out.println("Minimum Support Count: " + minSupportCount);

    String currentLprevPath = null; // Sẽ là đường dẫn đến output của L(k-1)
    List<String> intermediateOutputPaths = new ArrayList<>(); // Lưu các đường dẫn k_X để gom sau

    for (int k = 1; ; k++) {
      conf.setInt("k.value", k);
      String currentOutputSubPath = outputPathBase + "/k_" + k; // Thư mục output cho Lk
      intermediateOutputPaths.add(currentOutputSubPath); // Thêm vào list để gom sau
      Path currentOutputDir = new Path(currentOutputSubPath);

      if (fs.exists(currentOutputDir)) {
        fs.delete(currentOutputDir, true);
      }

      Job job = Job.getInstance(conf, "Apriori_k=" + k);
      job.setJarByClass(AprioriDriver.class);

      if (k == 1) {
        job.setMapperClass(L1Mapper.class);
        job.setReducerClass(L1Reducer.class);
        FileInputFormat.addInputPath(job, hdfsInputPath);
      } else {
        if (currentLprevPath == null) {
          System.out.println("Previous level (L" + (k-1) + ") path is null. This shouldn't happen after k=1. Stopping.");
          break;
        }

        Path lPrevHdfsPath = new Path(currentLprevPath);
        if (!fs.exists(lPrevHdfsPath)) {
          System.out.println("Path for L" + (k-1) + " (" + currentLprevPath + ") does not exist. Stopping.");
          break;
        }

        FileStatus[] lPrevFiles = fs.listStatus(lPrevHdfsPath);
        boolean lPrevHasContent = false;
        for(FileStatus file : lPrevFiles) {
          if (file.getPath().getName().startsWith("part-r-") && file.getLen() > 0) {
            job.addCacheFile(file.getPath().toUri()); // Thêm URI vào cache
            lPrevHasContent = true;
          }
        }

        if (!lPrevHasContent) {
          System.out.println("L" + (k-1) + " (from " + currentLprevPath + ") is empty or has no part-r- files with content. Stopping.");
          break;
        }

        job.setMapperClass(LkMapper.class);
        job.setReducerClass(LkReducer.class);
        FileInputFormat.addInputPath(job, hdfsInputPath);
      }

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, currentOutputDir);

      boolean success = job.waitForCompletion(true);
      if (!success) {
        System.err.println("Job k=" + k + " failed. Exiting.");
        // System.exit(1);
        return 1;
      }

      long numOutputRecords = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
      if (numOutputRecords == 0) {
        System.out.println("No frequent itemsets found for k=" + k + ". Stopping.");
        break;
      }
      currentLprevPath = currentOutputSubPath; // Output của job này sẽ là input (Lprev) cho job tiếp theo
    }

    // Gom tất cả các Lk vào thư mục final
    String finalOutputPath = outputPathBase + "/final_frequent_itemsets";
    Path finalOutDir = new Path(finalOutputPath);
    if (fs.exists(finalOutDir)) {
      fs.delete(finalOutDir, true);
    }
    fs.mkdirs(finalOutDir);
    System.out.println("\nConsolidating frequent itemsets into: " + finalOutputPath);

    for (String intermediatePathStr : intermediateOutputPaths) {
      Path intermediatePath = new Path(intermediatePathStr);
      if (fs.exists(intermediatePath) && fs.isDirectory(intermediatePath)) {
        FileStatus[] outputFiles = fs.listStatus(intermediatePath);
        for (FileStatus file : outputFiles) {
          if (file.getPath().getName().startsWith("part-r-") && file.getLen() > 0) {
            Path sourcePath = file.getPath();
            String kValueFromName = intermediatePath.getName(); // k_X
            Path destPath = new Path(finalOutputPath + "/" + kValueFromName + "_" + sourcePath.getName());
            if(fs.rename(sourcePath, destPath)){
              System.out.println("Moved " + sourcePath + " to " + destPath);
            } else {
              System.err.println("Failed to move " + sourcePath + " to " + destPath);
            }
          }
        }
        // Có thể xóa thư mục trung gian sau khi gom
        // fs.delete(intermediatePath, true);
      }
    }

    System.out.println("Apriori finished. Frequent itemsets consolidated in: " + finalOutputPath);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AprioriDriver(), args);
    System.exit(exitCode);
  }
}
