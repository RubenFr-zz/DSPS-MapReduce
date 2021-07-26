package Round1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Round1 {

    private static Logger logger;

    public static class MapperClass extends Mapper<LongWritable, Text, Gram2, IntWritable> {

        private int N;

        @Override
        public void setup(Context context) {
            N = 0;
            logger.warn("\nStarting Mapping for Round 1...\n");
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] gram = data[0].split(" ");
            IntWritable dec = new IntWritable(Integer.parseInt(data[1]));
            IntWritable occ = new IntWritable(Integer.parseInt(data[2]));

            if (gram.length != 2 || CommonConstants.stopWords.contains(gram[0]) || CommonConstants.stopWords.contains(gram[1]))
                context.getCounter(CommonConstants.COUNTERS.NOT_COUNTED).increment(1L);
            else {
                N += occ.get();
                context.write(new Gram2(new Text(gram[0]), new Text(gram[1]), dec), occ);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int curr_dec = CommonConstants.smallest_year;
            logger.warn("Broadcasting N = " + N + "\tFrom " + Thread.currentThread());

            while (curr_dec <= CommonConstants.largest_year) {
                context.write(new Gram2(curr_dec), new IntWritable(N));
                curr_dec += 10;
            }

            logger.warn("\nFinished Mapping for Round 1\n");
        }
    }

    public static class ReducerClass extends Reducer<Gram2, IntWritable, Text, Text> {

        @Override
        public void setup(Context context) {
            logger.warn("\nStarting Reducing for Round 1...\n");
        }

        @Override
        public void reduce(Gram2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int w1_w2_occ = 0;

            for (IntWritable value : values)
                w1_w2_occ += value.get();

            logger.warn("New key for reducer: " + key.getW1() + "_" + key.getW2() + " " + key.getDecade() + "\tOccurrences: " + w1_w2_occ);
            context.write(key.toText(), new Text(String.valueOf(w1_w2_occ)));
        }

        @Override
        public void cleanup(Context context) {
            logger.warn("\nFinished Reducing for Round 1\n");
        }
    }

    public static class CombinerClass extends Reducer<Gram2, IntWritable, Gram2, IntWritable> {

        @Override
        public void reduce(Gram2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int w1_w2_occ = 0;

            for (IntWritable value : values)
                w1_w2_occ += value.get();
            context.write(key, new IntWritable(w1_w2_occ));
        }
    }

    public static class PartitionerClass extends Partitioner<Gram2, IntWritable> {

        // Ensure that keys with the same decade are directed to the same reducer
        @Override
        public int getPartition(Gram2 key, IntWritable value, int numPartitions) {
            int dec = key.getDecade().get() / 10;
            return 200 - dec;
        }

    }

    public static void main(String[] args) throws Exception {
        logger = LoggerFactory.getLogger(Round1.class);
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Round 1");
        job.setJarByClass(Round1.class);

        // Set mapper
        job.setMapperClass(MapperClass.class);

        // Set partitioner
        job.setPartitionerClass(PartitionerClass.class);

        // Set combiner
        job.setCombinerClass(CombinerClass.class);

        // Set reducer
        job.setReducerClass(ReducerClass.class);
        job.setNumReduceTasks(42);

        // Set Mapper's Output format for key-value pair
        job.setMapOutputKeyClass(Gram2.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set Reducer's Output format for key-value pair
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output format for data
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // File Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Starting...
        logger.warn("Starting Round 1...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}