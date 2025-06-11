import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class KMeansClustering {

    private static final double[] CENTERS = {2.0, 6.0, 9.0};
    private static final Random RANDOM = new Random();

    public static class PointWritable implements Writable {
        private Text pointName;
        private DoubleWritable coordinate;
        private LongWritable count;

        public PointWritable() {
            pointName = new Text();
            coordinate = new DoubleWritable();
            count = new LongWritable(1);
        }

        public PointWritable(String name, double coord) {
            pointName = new Text(name);
            coordinate = new DoubleWritable(coord);
            count = new LongWritable(1);
        }

        public String getPointName() {
            return pointName.toString();
        }

        public double getCoordinate() {
            return coordinate.get();
        }

        public long getCount() {
            return count.get();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            pointName.write(out);
            coordinate.write(out);
            count.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            pointName.readFields(in);
            coordinate.readFields(in);
            count.readFields(in);
        }
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, DoubleWritable, PointWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;
            String pointName = parts[0];
            double coordinate;
            try {
                coordinate = Double.parseDouble(parts[1]);
            } catch (NumberFormatException e) {
                return;
            }

            double minDistance = Double.MAX_VALUE;
            ArrayList<Double> closestCenters = new ArrayList<>();
            for (double center : CENTERS) {
                double distance = Math.abs(coordinate - center);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCenters.clear();
                    closestCenters.add(center);
                } else if (distance == minDistance) {
                    closestCenters.add(center);
                }
            }
            double selectedCenter = closestCenters.get(RANDOM.nextInt(closestCenters.size()));

            context.write(new DoubleWritable(selectedCenter), new PointWritable(pointName, coordinate));
        }
    }

    public static class KMeansReducer extends Reducer<DoubleWritable, PointWritable, Text, Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<PointWritable> values, Context context) throws IOException, InterruptedException {
            double oldCenter = key.get();
            ArrayList<String> pointNames = new ArrayList<>();
            double sumCoordinates = 0.0;
            long count = 0;

            for (PointWritable value : values) {
                pointNames.add(value.getPointName());
                sumCoordinates += value.getCoordinate();
                count += value.getCount();
            }

            double newCenter = count > 0 ? sumCoordinates / count : oldCenter;

            StringBuilder output = new StringBuilder();
            output.append(oldCenter).append("\t").append(newCenter).append("\t");
            for (int i = 0; i < pointNames.size(); i++) {
                output.append(pointNames.get(i));
                if (i < pointNames.size() - 1) {
                    output.append(" ");
                }
            }

            context.write(new Text(), new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeansClustering.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(PointWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
