package heigvd.bda.labs.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class InvertedIndex extends Configured implements Tool {
	
	public final static IntWritable ONE = new IntWritable(1);
	
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
	    return Arrays.copyOf(result.toArray(),result.size(),String[].class);
	}
		
	/**
	 * InvertedIndex Constructor.
	 * 
	 * @param args
	 */
	public InvertedIndex(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: InvertedIndex <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		numReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputPath = new Path(args[2]);
	}
	
	/**
	 * InvertedIndex mapper: (doc, line) => (word, (doc, tf))
	 */
	static class IIMapper extends Mapper<LongWritable, Text, /* TODO */, /* TODO */> {
		
		private String filename = "default";
		// TODO : DEFINE VARIABLES
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			// TODO : INITIALIZE VARIABLES
		}
			
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {			
			
			// USED FOR BIBLE & SHAKES
			for (String word : InvertedIndex.words(value.toString()))	{
				// TODO : COMPLETE MAP
			}	
							
			// USED FOR WIKIPEDIA
			String line = value.toString();
			if (line.startsWith("###")) {
				filename = line.substring(36);
			}			
			else {	
				for (String word : InvertedIndex.words(value.toString()))	{
					// TODO : COMPLETE MAP
				}			
			}
		}	
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {			
			// TODO : CLEANUP				
			super.cleanup(context);
		}		

	}

	/**
	 * InvertedIndex reducer: (word, {(doc, tf)}) => (word, df {doc, tf})
	 */
	static class IIReducer extends Reducer</* TODO */, /* TODO */, /* TODO */, /* TODO */> {
		
		// TODO : DEFINE VARIABLES
	    
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			// TODO : INITIALIZE VARIABLES
		}
		
		@Override
		protected void reduce(Text key, Iterable<TextPair> values, Context context)
				throws IOException, InterruptedException {
			
			// TODO : COMPLETE REDUCE
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {			
			// TODO : COMPLETE CLEANUP				
			super.cleanup(context);
		}	
		
	}

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		Job job = new Job(conf,"Inverted Index");
		
		job.setMapperClass(IIMapper.class);	
		job.setReducerClass(IIReducer.class);
	
		job.setMapOutputKeyClass(/* TODO */);
		job.setMapOutputValueClass(/* TODO */);
		
		job.setOutputKeyClass(/* TODO */);
		job.setOutputValueClass(/* TODO */);
		
		TextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(numReducers);
		
		job.setJarByClass(InvertedIndex.class);

		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Configuration(), new InvertedIndex(args), args);
	}
}

