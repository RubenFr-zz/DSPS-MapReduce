package Round3;

import LocalApplication.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Round3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Gram2, DoubleWritable> {

        @Override
        public void map(LongWritable line_number, Text line, Context context) throws IOException, InterruptedException {

            String[] data = line.toString().split("\t");
            String[] key = data[0].split(" ");
            int dec = Integer.parseInt(key[0]);
            double pmi = Double.parseDouble(data[1]);

            // Submit K = dec w1 w2, V = occ (to count the recurrence of this 2gram in the decade)
            context.write(new Gram2(dec), new DoubleWritable(pmi));
            context.write(new Gram2(new Text(key[1]), new Text(key[2]), dec), new DoubleWritable(pmi));
        }

    }

    public static class ReducerClass extends Reducer<Gram2, DoubleWritable, Text, Text> {

        private double minPmi;
        private double relMinPmi;
        private double pmi_decade;

        @Override
        public void setup(Context context) {
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
            relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));
            pmi_decade = 0;
        }

        @Override
        public void reduce(Gram2 key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            // 1. If new decade compute key
            if (key.is_N()) {

                int sum = 0;
                for (DoubleWritable value : values)
                    sum += value.get();

                pmi_decade = sum;
            }

            // 2. Compare and write only PMIs above given parameters
            double pmi = values.iterator().next().get();
            if ( pmi >= minPmi && pmi / pmi_decade >= relMinPmi)
                context.write(key.toText(), new Text("Relative PMI: " + pmi));
        }
    }


    public static class PartitionerClass extends Partitioner<Text, IntWritable> {

        // Ensure that keys with the same decade are directed to the same reducer
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.toString().split(" ")[0].hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Round3.class);

        if (args.length != 5) {
            logger.error("Usage: java -jar Round1.jar <input> <output> <minPmi> <relMinPmi>\n");
            for (int i = 0; i < args.length; i++)
                logger.error("Argument " + i + ": " + args[i]);
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("minPmi", args[3]);
        conf.set("relMinPmi", args[4]);

        Job job = Job.getInstance(conf, "Round 1");
        job.setJarByClass(Round3.class);

        // Set mapper
        job.setMapperClass(MapperClass.class);

        // Set partitioner
        job.setPartitionerClass(PartitionerClass.class);

        // Set reducer
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(Constants.NumReduceTasks);

        // Set Mapper's Output format for key-value pair
        job.setMapOutputKeyClass(Gram2.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set Reducer's Output format for key-value pair
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output format for data
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // File Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Starting...
        logger.warn("Starting Round 3...");
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        logger.warn("Finished Round 3!");

        System.exit(exitCode);
    }
}