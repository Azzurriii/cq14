import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question10 {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            try {
                String[] parts = line.split("\t", 3);
                if (parts.length == 3) {
                    String table = parts[0].trim();
                    String itemKey = parts[1].trim();
                    String itemValue = parts[2].trim();

                    outputKey.set(itemKey);

                    // Tag the value with table name for identification in reducer
                    if ("FoodPrice".equals(table)) {
                        outputValue.set("P:" + itemValue);  // P for Price
                    } else if ("FoodQuantity".equals(table)) {
                        outputValue.set("Q:" + itemValue);  // Q for Quantity
                    }

                    context.write(outputKey, outputValue);
                }
            } catch (Exception e) {
                System.err.println("Error parsing line: " + line);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String priceValue = null;
            List<String> quantityValues = new ArrayList<>();

            // Separate price and quantity values
            for (Text value : values) {
                String val = value.toString();
                if (val.startsWith("P:")) {
                    priceValue = val.substring(2);  // Remove "P:" prefix
                } else if (val.startsWith("Q:")) {
                    quantityValues.add(val.substring(2));  // Remove "Q:" prefix
                }
            }

            // RIGHT OUTER JOIN: Include all quantity records
            // If no quantity records, skip this key
            if (quantityValues.isEmpty()) {
                return;
            }

            // For each quantity record, create output with price (or null if no price)
            for (String quantityValue : quantityValues) {
                String price = (priceValue != null) ? priceValue : "null";

                // Format: key \t price \t quantity
                result.set(price + "\t" + quantityValue);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Question10 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "right outer join");
        job.setJarByClass(Question10.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}