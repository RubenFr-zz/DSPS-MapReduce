package Round1;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Round1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Gram2, IntWritable> {

        private int N;
//        private int sent;

        @Override
        public void setup(Context context) {
            N = 0;
//            sent = 0;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");
            String[] gram = data[0].split(" ");
            IntWritable dec = new IntWritable(Integer.parseInt(data[1]));
            IntWritable occ = new IntWritable(Integer.parseInt(data[2]));

//            if (!isValid(gram) || sent > 10000)
            if (!isValid(gram))
                context.getCounter(Constants.COUNTERS.NOT_COUNTED).increment(1L);
            else {
                context.getCounter(Constants.COUNTERS.COUNTED).increment(1L);
                N += occ.get();
//                sent += 1;

                // K = dec w1 *, V = occ (to count the 2grams starting with w1 in the entire corpus)
                // K = dec * w2, V = occ (to count the 2grams ending with w2 in the entire corpus)
                context.write(new Gram2(new Text(gram[0]), new Text("*"), 1), occ);
                context.write(new Gram2(new Text("*"), new Text(gram[1]), 2), occ);

                // K = dec w1 w2, V = occ (to count the recurrence of this 2gram in the decade)
                context.write(new Gram2(new Text(gram[0]), new Text(gram[1]), dec,1), occ);
                context.write(new Gram2(new Text(gram[0]), new Text(gram[1]), dec,2), occ);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            // Send to every reducer the value of N
            for (int i = 0; i < Constants.NumReduceTasks; ++i) {
                context.write(new Gram2(i,1), new IntWritable(N));  // w1 *
                context.write(new Gram2(i,2), new IntWritable(N));  // * w2
            }
        }

        private boolean isValid(String[] gram) {
            return gram.length == 2 &&
                    isValid(gram[0]) &&
                    isValid(gram[1]);
        }

        private boolean isValid(String word) {
            return word.matches ("^[א-ת]+$") && !Constants.stopWords.contains(word);
        }
    }

    public static class ReducerClass extends Reducer<Gram2, IntWritable, Text, Text> {

        private IntWritable N;
        private IntWritable last_occ;

        @Override
        public void setup(Context context) {
            N = new IntWritable();
            last_occ = new IntWritable();
        }

        @Override
        public void reduce(Gram2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // 1. Merge Counts
            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            // 2. Check if N
            if (key.is_N())
                N.set(sum);

            // 3. Check if new W1
            else if (key.isW1Star() || key.isW2Star())
                last_occ.set(sum);

            // 4. If not add c(w1/2) to the value -> dec w1 w2 TAB c(w1,w2) c(w1/2) N
            else
                context.write(key.toText(), new Text(sum + " " +  last_occ + " " + N));

        }
    }

    public static class CombinerClass extends Reducer<Gram2, IntWritable, Gram2, IntWritable> {

        @Override
        public void reduce(Gram2 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            context.write(key, new IntWritable(sum));

        }
    }

    public static class PartitionerClass extends Partitioner<Gram2, IntWritable> {

        @Override
        public int getPartition(Gram2 key, IntWritable value, int numPartitions) {
            if (key.is_N()) {
                int part = key.getDecade() % Constants.NumReduceTasks;
                if (key.getType() == 2)
                    part += Constants.NumReduceTasks;
                return part;
            }

            // Divide Type 1 and Type 2
            if (key.getType() == 1)
                return Math.abs(key.getW1().hashCode()) % Constants.NumReduceTasks;
            if (key.getType() == 2)
                return Math.abs(key.getW2().hashCode() % Constants.NumReduceTasks) + Constants.NumReduceTasks;

            return -1;  // ILLEGAL
        }
    }

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Round1.class);

        if (args.length != 3) {
            logger.error("Usage: java -jar Round1.jar <input> <output>\n");
            for (int i = 0; i < args.length; i++)
                logger.error("Argument " + i + ": " + args[i]);
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
        job.setNumReduceTasks(Constants.NumReduceTasks * 2);

        // Set Mapper's Output format for key-value pair
        job.setMapOutputKeyClass(Gram2.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set Reducer's Output format for key-value pair
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and Output format for data
//        job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // File Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Starting...
        logger.warn("Starting Round 1...");
        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        logger.warn("Finished Round 1!");

        System.exit(exitCode);
    }
}