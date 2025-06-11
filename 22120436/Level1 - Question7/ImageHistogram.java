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

import java.io.IOException;

public class ImageHistogram {

    public static class HistogramMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private IntWritable pixelValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] pixels = value.toString().trim().split("\\s+");
            for (String pixel : pixels) {
                try {
                    int val = Integer.parseInt(pixel);
                    pixelValue.set(val);
                    context.write(pixelValue, one);
                } catch (NumberFormatException e) {
                }
            }
        }
    }

    public static class HistogramReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Image Histogram");
        job.setJarByClass(ImageHistogram.class);

        job.setMapperClass(HistogramMapper.class);
        job.setCombinerClass(HistogramReducer.class); 
        job.setReducerClass(HistogramReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
