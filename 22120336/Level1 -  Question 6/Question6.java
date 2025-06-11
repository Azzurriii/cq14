import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question6 {

    public static class DistanceMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private int queryPoint;
        private IntWritable distance = new IntWritable();
        private Text pointName = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            queryPoint = conf.getInt("query.point", 4);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            try {
                // Split the line into point name and coordinate
                String[] parts = line.split("\t");

                if (parts.length == 2) {
                    String point = parts[0].trim();
                    int coordinate = Integer.parseInt(parts[1].trim());

                    // Calculate the distance from the query point
                    int dist = Math.abs(coordinate - queryPoint);

                    distance.set(dist);
                    pointName.set(point);
                    // Emit <distance, pointName>
                    context.write(distance, pointName);
                }
            } catch (NumberFormatException e) {
                System.err.println("Error parsing line: " + line);
            }
        }
    }

    public static class DistanceReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private Text result = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> points = new ArrayList<>();

            // Collect all points that have the same distance
            for (Text value : values) {
                points.add(value.toString());
            }

            // Sort the points alphabetically
            Collections.sort(points);

            String pointsList = String.join(" ", points);

            // Set the result as a space-separated string of point names
            result.set(pointsList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Question6 <input_path> <output_path> <query_point>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        int queryPoint = Integer.parseInt(args[2]);
        conf.setInt("query.point", queryPoint);

        Job job = Job.getInstance(conf, "distance grouping");
        job.setJarByClass(Question6.class);

        job.setMapperClass(DistanceMapper.class);
        job.setCombinerClass(DistanceReducer.class);
        job.setReducerClass(DistanceReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}