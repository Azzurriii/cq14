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

public class Question4 {

    public static class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text groupId = new Text();
        private Text pointValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            try {
                // Split
                String[] parts = line.split("\t", 2);
                if (parts.length == 2) {
                    String group = parts[0].trim();
                    String point = parts[1].trim();
                    
                    // Validate point is an integer
                    Integer.parseInt(point);
                    
                    groupId.set(group);
                    pointValue.set(point);
                    
                    // Emit group as key and point as value
                    context.write(groupId, pointValue);
                }
            } catch (NumberFormatException e) {
                System.err.println("Error parsing point value in line: " + line);
            }
        }
    }

    public static class GroupReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Integer> points = new ArrayList<>();

            // Collect all points for this group
            for (Text value : values) {
                try {
                    int point = Integer.parseInt(value.toString());
                    points.add(point);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing point: " + value.toString());
                }
            }

            if (points.isEmpty()) {
                return;
            }

            // Sort points to find centroid (median)
            Collections.sort(points);

            // Calculate center (mean)
            double sum = 0;
            for (int point : points) {
                sum += point;
            }
            double center = sum / points.size();

            // Calculate centroid (median)
            int centroid;
            int size = points.size();
            if (size % 2 == 1) {
                // Odd number of points - middle element
                centroid = points.get(size / 2);
            } else {
                // Even number of points - average of two middle elements
                centroid = (points.get(size / 2 - 1) + points.get(size / 2)) / 2;
            }

            // Format output: group \t center \t centroid
            String output = String.format("%.2f\t%d", center, centroid);
            result.set(output);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: Question4 <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "group center and centroid");
        job.setJarByClass(Question4.class);

        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}