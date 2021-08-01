package Round2;

import LocalApplication.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

public class Round2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Gram2, Text> {

        @Override
        public void map(LongWritable line_number, Text line, Context context) throws IOException, InterruptedException {

            String[] data = line.toString().split("\t");
            String[] key = data[0].split(" ");
            int dec = Integer.parseInt(key[0]);

            context.write(new Gram2(key[1], key[2], new IntWritable(dec)), new Text(data[1]));
        }

        @Override
        public void cleanup(Context context) {
        }
    }

    public static class ReducerClass extends Reducer<Gram2, Text, Text, Text> {

        @Override
        public void reduce(Gram2 key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 1. Compute PMI
            Iterator<Text> it = values.iterator();
            String[] first = it.next().toString().split(" ");
            String[] second = it.next().toString().split(" ");
            double npmi = compute_npmi(first[2], first[0], first[1], second[1]);

            if (npmi >= 0)
                context.getCounter(Constants.COUNTERS.POSITIVE).increment(1L);
            else
                context.getCounter(Constants.COUNTERS.NEGATIVE).increment(1L);

            // 2. Write the result
            context.write(key.toText(), new Text(String.valueOf(npmi)));
        }

        public double compute_npmi(String N_str, String c_w1_w2_str, String c_w1_str, String c_w2_str) {
            double N = Double.parseDouble(N_str);
            double c_w1_w2 = Double.parseDouble(c_w1_w2_str);
            double c_w1 = Double.parseDouble(c_w1_str);
            double c_w2 = Double.parseDouble(c_w2_str);
            double pmi = Math.log10(c_w1_w2) + Math.log10(N) - Math.log10(c_w1) - Math.log10(c_w2);
            return pmi / (- Math.log10(c_w1_w2 / N));
        }
    }

    public static class PartitionerClass extends Partitioner<Gram2, Text> {

        // Ensure that keys with the same decade are directed to the same reducer
        @Override
        public int getPartition(Gram2 key, Text value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(Round2.class);

        if (args.length != 3) {
            logger.error("Usage: java -jar Round2.jar Round2 <input> <output>\n");
            for (int i = 0; i < args.length; i++)
                logger.error("Argument " + i + ": " + args[i]);
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Round 2");
        job.setJarByClass(Round2.class);

        // Set mapper
        job.setMapperClass(MapperClass.class);

        // Set partitioner
        job.setPartitionerClass(PartitionerClass.class);

        // Set reducer
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(Constants.NumReduceTasks);

        // Set Mapper's Output format for key-value pair
        job.setMapOutputKeyClass(Gram2.class);
        job.setMapOutputValueClass(Text.class);

        // Set Reducer's Output format for key-value pair
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output format for data
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // File Input and Output paths
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        for (int i = 0; i < Constants.NumReduceTasks * 2; ++i)
            MultipleInputs.addInputPath(job, new Path(args[1] + "/part-r-0000" + i), TextInputFormat.class);

        // Starting...
        logger.warn("Starting Round 2...");
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        logger.warn("Finished Round 2!");

        System.exit(exitCode);
    }
}