package heigvd.bda.labs.relfreq;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import heigvd.bda.labs.utils.TextPair;

public class OrderInversion extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputPath;

    private final static String ASTERISK = "\0";

    /**
     * OrderInversion Constructor.
     *
     * @param args
     */
    public OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        numReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputPath = new Path(args[2]);
    }

    /**
     * Utility to split a line of text in words.
     * The text is first transformed to lowercase, all non-alphanumeric characters are removed.
     * All spaces are removed and the text is tokenized using Java StringTokenizer.
     *
     * @param text what we want to split
     * @return words in text as an Array of String
     */
    public static String[] words(String text) {
        text = text.toLowerCase();
        text = text.replaceAll("[^a-z]+", " ");
        text = text.replaceAll("^\\s+", "");
        StringTokenizer st = new StringTokenizer(text);
        ArrayList<String> result = new ArrayList<String>();
        while (st.hasMoreTokens())
            result.add(st.nextToken());
        return Arrays.copyOf(result.toArray(), result.size(), String[].class);
    }

    public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {

        @Override
        public int getPartition(TextPair key, IntWritable value, int numPartitions) {
            // _TODO: implement getPartition such that pairs with the same first element
            //       will go to the same reducer.
            return Math.abs(key.getFirst().hashCode() % numPartitions);
        }
    }

    public static class OrderInversionMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private Text word1;
        private Text word2;
        private TextPair combinedKey;
        private IntWritable countValue;
        private Map<String, Integer> wordCombining;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            word1 = new Text();
            word2 = new Text();
            combinedKey = new TextPair();
            combinedKey.set(word1, word2);
            countValue = new IntWritable(1);
            wordCombining = new HashMap<>();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String[] words = words(value.toString());

            // Emit combining of the two words
            for (int i = 1; i < words.length; i++) {
                word1.set(words[i - 1]);
                word2.set(words[i]);
                context.write(combinedKey, countValue);
            }

            // Emit each words separately
            for (String word : words) {
                if (!wordCombining.containsKey(word)) {
                    wordCombining.put(word, 1);
                } else {
                    wordCombining.put(word, wordCombining.get(word) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Iterator<Map.Entry<String, Integer>> itr = wordCombining.entrySet().iterator();
            word2.set(ASTERISK);
            while (itr.hasNext()) {
                Map.Entry<String, Integer> entry = itr.next();
                word1.set(entry.getKey());
                countValue.set(entry.getValue());
                context.write(combinedKey, countValue);
            }
        }
    }

    public static class OrderInversionReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

        private long countWord;
        private DoubleWritable relativeCount = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            if (key.getSecond().toString().equals(ASTERISK)) {
                countWord = sum;
            } else {
                relativeCount.set(sum / (double) countWord);
                context.write(key, relativeCount);
            }
            System.out.println("Key is (" + key.getFirst().toString() + ", " + key.getSecond().toString() + ")");
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Order Inversion");

        job.setJarByClass(OrderInversion.class);

        job.setMapperClass(OrderInversionMapper.class);
        job.setReducerClass(OrderInversionReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        // _TODO: set the partitioner and sort order
        job.setPartitionerClass(PartitionerTextPair.class);

        job.setNumReduceTasks(numReducers);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}
