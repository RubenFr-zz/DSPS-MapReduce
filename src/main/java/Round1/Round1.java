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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Round1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Gram2, IntWritable> {

        private int N;

        @Override
        public void setup(Context context) {
            N = 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            String[] gram = data[0].split(" ");
            IntWritable dec = new IntWritable(Integer.parseInt(data[1]));
            IntWritable occ = new IntWritable(Integer.parseInt(data[2]));

            if (gram.length != 2 || Constants.stopWords.contains(gram[0]) || Constants.stopWords.contains(gram[1]))
                context.getCounter(Constants.COUNTERS.NOT_COUNTED).increment(1L);
            else {
                N += occ.get();

                // Broadcast to every reducer K = dec w1 *, V = occ (to count the 2grams starting with w2 in the entire corpus)
                broadcast(new Gram2(new Text(gram[0]), new Text(gram[1])), occ, context);

                // Submit K = dec w1 w2, V = occ (to count the recurrence of this 2gram in the decade)
                context.write(new Gram2(new Text(gram[0]), new Text(gram[1]), dec), occ);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int curr_dec = Constants.smallest_year;

            while (curr_dec <= Constants.largest_year) {
                context.write(new Gram2(curr_dec), new IntWritable(N));
                curr_dec += 10;
            }
        }

        private void broadcast(Gram2 key, IntWritable val, Context context) throws IOException, InterruptedException {
            key.setW2(new Text("*"));
            int curr_dec = Constants.smallest_year;

            while (curr_dec <= Constants.largest_year) {
                context.write(key.setDecade(curr_dec), val);
                curr_dec += 10;
            }
        }
    }

    public static class ReducerClass extends Reducer<Gram2, IntWritable, Text, Text> {

        private IntWritable lastW1_occ;

        @Override
        public void setup(Context context) {
            lastW1_occ = new IntWritable();
        }

        @Override
        public void reduce(Gram2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 1. Merge Counts
            int w1_w2_occ = 0;

            for (IntWritable value : values)
                w1_w2_occ += value.get();

            // 2. Check if N
            if (key.is_N())
                context.write(key.toText(), new Text(String.valueOf(w1_w2_occ)));

                // 3. Check if new W1
            else if (key.getW2().equals("*"))
                lastW1_occ.set(w1_w2_occ);

                // 4. If not add c(w1) to the value -> dec w1 w2 TAB c(w1,w2) c(w1)
            else
                context.write(key.toText(), new Text(w1_w2_occ + " " + lastW1_occ));

        }

        @Override
        public void cleanup(Context context) {
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
        Logger logger = LoggerFactory.getLogger(Round1.class);

        if (args.length != 2) {
            logger.error("Usage: java -jar Round1.jar <input> <output>\n");
            System.exit(-1);
        }

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
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        logger.warn("Finished Round 1!");

        System.exit(exitCode);
    }
}