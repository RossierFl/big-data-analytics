import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;


public class Main {

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		Configuration config = new Configuration();
	    Path path = new Path("/home/rossier/invertedIndex/part-r-00002");
	    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
	    Text key = (Text) reader.getKeyClass().newInstance();
	    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = (PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>) reader.getValueClass().newInstance();
	    long position = reader.getPosition();
	    String result = "";
	    while(reader.next(key,value))
	    {
	    	String keyString = key.toString();
	    	if(keyString.equals("silver")){
	    		System.out.println("Key is: "+key +" value is: "+value.getKey()+ "map is "+value.getValue()+"\n");
	    		ArrayListWritable<PairOfInts> values = value.getValue();
	    		HashMap<Integer, Integer> histo = new HashMap<Integer, Integer>();
	    		for(PairOfInts pair : values){
	    			if(histo.containsKey(pair.getValue())){
	    				histo.put(pair.getValue(), histo.get(pair.getValue()) + 1);
	    			}
	    			else histo.put(pair.getValue(), 1);
	    		}
	    		System.out.println(histo.toString());
	    	}
	    }
	}

}
