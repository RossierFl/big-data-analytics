package heigvd.bda.labs.bigram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import heigvd.bda.labs.utils.StringToIntMapWritable;
import heigvd.bda.labs.utils.TextPair;

public class Stripes extends Configured implements Tool {

	public final static IntWritable ONE = new IntWritable(1);
	
	private int numReducers;
	private Path inputPath;
	private Path outputPath;
	
	/**
	 * Pairs Constructor.
	 * 
	 * @param args
	 */
	public Stripes(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: Strips <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputPath = new Path(args[2]);
	}
	
	/**
	 * Utility to split a line of text in words.
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
	
	public static class StripesMapper extends Mapper<LongWritable, Text, Text, StringToIntMapWritable> {

		private Map<String, StringToIntMapWritable> map;
		private Text keyRes;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			map = new HashMap<String, StringToIntMapWritable>();
			keyRes = new Text();
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

			String[] tokens = Stripes.words(value.toString());
			
			for (int i = 0; i < tokens.length-1; i++) {
				if(map.containsKey(tokens[i])){
					map.get(tokens[i]).increment(tokens[i+1]);
				}else{
					StringToIntMapWritable mapForToken = new StringToIntMapWritable();
					mapForToken.increment(tokens[i+1]);
					map.put(tokens[i], mapForToken);
				}			
			}
			Iterator it = map.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        keyRes.set((String)pair.getKey());
		        context.write(keyRes, (StringToIntMapWritable)pair.getValue());
		        it.remove();
		    }
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, StringToIntMapWritable>.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			
		}
	}
	
	public static class StripesCombiner extends Reducer<Text, StringToIntMapWritable, Text, StringToIntMapWritable> {

		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<StringToIntMapWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			StringToIntMapWritable resMap = new StringToIntMapWritable();
			for (StringToIntMapWritable value : values) {	
				resMap.sum(value);
			}
			context.write(key, resMap);
			
		}
	}

	public static class StripesReducer extends Reducer<Text, StringToIntMapWritable, TextPair, IntWritable> {

		private IntWritable res;
		private TextPair resPair;
		private Text resKey;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			res = new IntWritable();
			resPair = new TextPair();
			resKey = new Text();
		}
		
		@Override
		public void reduce(Text key, Iterable<StringToIntMapWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			StringToIntMapWritable resMap = new StringToIntMapWritable();
			for (StringToIntMapWritable value : values) {	
				resMap.sum(value);
			}
			Iterator it = resMap.getHashMap().entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        resKey.set((String)pair.getKey());
		        resPair.set(key, resKey);
		        res.set((int)pair.getValue());
		        context.write(resPair, res);
		        it.remove(); // avoids a ConcurrentModificationException
		    }
			
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "Pairs");

		job.setMapperClass(StripesMapper.class);
		//job.setCombinerClass(StripesCombiner.class);
		job.setReducerClass(StripesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringToIntMapWritable.class);

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numReducers);

		job.setJarByClass(Stripes.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
		System.exit(res);
	}
}
