package heigvd.bda.labs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

/**
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int.
 */
public class StringToDoubleMapWritable implements Writable {
	
	private HashMap<String, Double> hm = new HashMap<String, Double>();

	public void clear() {
		hm.clear();
	}

	public String toString() {
		return hm.toString();
	}
	
	public HashMap<String, Double> getHashMap(){
		return hm;
	}

	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		hm.clear();
		for (int i=0; i<len; i++) {
			int l = in.readInt();
			byte[] ba = new byte[l];
			in.readFully(ba);
			String key = new String(ba);
			Double value = in.readDouble();
			hm.put(key, value);
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(hm.size());
		Iterator<Entry<String, Double>> it = hm.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<String,Double> pairs = (Map.Entry<String,Double>)it.next();
			String k = (String) pairs.getKey();
			Double v = (Double)pairs.getValue();	        
			out.writeInt(k.length());
			out.writeBytes(k);
			out.writeDouble(v);
		}
	}

	public void increment(String t) {
		double count = 1;
		if (hm.containsKey(t)) {
			count = hm.get(t) + count;
		}
		hm.put(t, count);
	}

	public void increment(String t, double value) {
		double count = value;
		if (hm.containsKey(t)) {
			count = hm.get(t) + count;
		}
		hm.put(t, count);
	}

	public void sum(StringToDoubleMapWritable h) {
		Iterator<Entry<String, Double>> it = h.hm.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String,Double> pairs = (Map.Entry<String,Double>)it.next();
			String k = (String) pairs.getKey();
			double v = (double)pairs.getValue();
			increment(k, v);
		}
	}
	
}
