import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class FoodJoin {

    public static class TableData implements Writable {
        private Text table;
        private Text value;

        public TableData() {
            table = new Text();
            value = new Text();
        }

        public TableData(String table, String value) {
            this.table = new Text(table);
            this.value = new Text(value);
        }

        public String getTable() {
            return table.toString();
        }

        public String getValue() {
            return value.toString();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            table.write(out);
            value.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            table.readFields(in);
            value.readFields(in);
        }
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, TableData> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split("\\s+");
            if (parts.length != 3) return;
            String table = parts[0];
            String itemKey = parts[1];
            String val = parts[2];
            if (!table.equals("FoodPrice") && !table.equals("FoodQuantity")) return;
            context.write(new Text(itemKey), new TableData(table, val));
        }
    }

    public static class JoinReducer extends Reducer<Text, TableData, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TableData> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> prices = new ArrayList<>();
            ArrayList<String> quantities = new ArrayList<>();
            for (TableData data : values) {
                String table = data.getTable();
                String value = data.getValue();
                if (table.equals("FoodPrice")) {
                    prices.add(value);
                } else if (table.equals("FoodQuantity")) {
                    quantities.add(value);
                }
            }
            for (String price : prices) {
                for (String quantity : quantities) {
                    context.write(key, new Text(price + "\t" + quantity));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Food Join");
        job.setJarByClass(FoodJoin.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
