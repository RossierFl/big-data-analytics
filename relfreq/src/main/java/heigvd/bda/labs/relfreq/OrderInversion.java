package heigvd.bda.labs.relfreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

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
	    return Arrays.copyOf(result.toArray(),result.size(),String[].class);
	}	
	
	public static class PartitionerTextPair extends Partitioner<TextPair, IntWritable> {
		
		@Override
		public int getPartition(TextPair key, IntWritable value, int numPartitions) {
			
      		// TODO: implement getPartition such that pairs with the same first element
      		//       will go to the same reducer.
      		return 0;		
		
		}
	}
	
	public static class OrderInversionMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

      		// TODO: implement the map method
		}
	}

	public static class OrderInversionReducer extends Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

		    // TODO: implement the reduce method			

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

		// TODO: set the partitioner and sort order

		job.setNumReduceTasks(numReducers);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
		System.exit(res);
	}
}
