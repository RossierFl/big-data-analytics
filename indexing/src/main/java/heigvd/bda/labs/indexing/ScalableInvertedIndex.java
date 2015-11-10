package heigvd.bda.labs.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

import java.io.IOException;
import java.util.*;

public class ScalableInvertedIndex extends Configured implements Tool {

    public final static IntWritable ONE = new IntWritable(1);
    public final static String ASTERIX = "\0";

    private int numReducers;
    private Path inputPath;
    private Path outputPath;

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
        while (st.hasMoreTokens()) {
            result.add(st.nextToken());
        }
        return Arrays.copyOf(result.toArray(), result.size(), String[].class);
    }

    /**
     * InvertedIndex Constructor.
     *
     * @param args
     */
    public ScalableInvertedIndex(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ScalableInvertedIndex <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        numReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputPath = new Path(args[2]);
    }

    /**
     * InvertedIndex mapper: (doc, line) => ((word, doc), tf) and ((word, *), 1)
     */
    static class IIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

        private String filename = "default";
        private Object2IntFrequencyDistribution<String> counts;
        private PairOfStrings composedKey;
        private IntWritable tf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            counts = new Object2IntFrequencyDistributionEntry<>();
            composedKey = new PairOfStrings();
            tf = new IntWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            counts.clear();
            /*
            // USED FOR BIBLE & SHAKES
            for (String word : ScalableInvertedIndex.words(value.toString())) {
                counts.increment(word);
            }
            // emit postings
            for (PairOfObjectInt<String> e : counts) {
                // emit normal key-value pairs
                composedKey.set(e.getLeftElement(), String.valueOf(key.get()));
                tf.set(e.getRightElement());
                context.write(composedKey, tf);

                // emit special key-value paris to keep track of df
                composedKey.set(e.getLeftElement(), ASTERIX);
                context.write(composedKey, ONE);
            }
            */

            // USED FOR WIKIPEDIA
            String line = value.toString();
            if (line.startsWith("###")) {
                filename = line.substring(36);
            } else {
                for (String word : InvertedIndex.words(value.toString())) {
                    counts.increment(word);
                }
                // emit postings
                for (PairOfObjectInt<String> e : counts) {
                    // emit normal key-value pairs
                    composedKey.set(e.getLeftElement(), filename);
                    tf.set(e.getRightElement());
                    context.write(composedKey, tf);

                    // emit special key-value paris to keep track of df
                    composedKey.set(e.getLeftElement(), ASTERIX);
                    context.write(composedKey, ONE);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * InvertedIndex reducer: ((word, doc), {tf}) and ((word, *), {1}) => (word, df {doc, tf})
     */
    static class IIReducer extends Reducer<PairOfStrings, IntWritable, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfStringInt>>> {

        private IntWritable dfW;
        private String previousWord = null;
        private Text previousWordText;
        private ArrayListWritable<PairOfStringInt> postings;
        private PairOfWritables<IntWritable, ArrayListWritable<PairOfStringInt>> postingsWithDf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            dfW = new IntWritable();
            previousWord = null;
            previousWordText = new Text();
            postings = new ArrayListWritable<>();
            postingsWithDf = new PairOfWritables<>();
            dfW = new IntWritable();
        }

        @Override
        protected void reduce(PairOfStrings key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            String word = key.getLeftElement();
            String docno = key.getRightElement();

            // Emit
            if (previousWord != null && !word.equals(previousWord)) {
                postingsWithDf.set(dfW, postings);
                previousWordText.set(previousWord);
                context.write(previousWordText, postingsWithDf);
                postings.clear();
            }

            // Sum value
            // Could be the df or the tf
            int sumOfValue = 0;
            Iterator<IntWritable> itr = value.iterator();
            while (itr.hasNext()) {
                sumOfValue += itr.next().get();
            }

            // Compute df
            if (docno.equals(ASTERIX)) {
                dfW.set(sumOfValue);
            }

            // Merge postings
            else {
                postings.add(new PairOfStringInt(docno, sumOfValue));
            }

            previousWord = word;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            if (previousWord != null) {
                postingsWithDf.set(dfW, postings);
                previousWordText.set(previousWord);
                context.write(previousWordText, postingsWithDf);
            }
        }
    }

    static class IIPartitioner extends Partitioner<PairOfStrings, IntWritable> {

        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int i) {
            return Math.abs(key.getLeftElement().hashCode() % i);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setMapperClass(IIMapper.class);
        job.setReducerClass(IIReducer.class);
        job.setPartitionerClass(IIPartitioner.class);

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(numReducers);

        job.setJarByClass(ScalableInvertedIndex.class);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String args[]) throws Exception {
        ToolRunner.run(new Configuration(), new ScalableInvertedIndex(args), args);
    }
}

