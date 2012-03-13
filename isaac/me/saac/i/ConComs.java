package me.saac.i;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

public class ConComs {

    public static enum CHANGED_NODES_COUNTER {CHANGED_NODES};

    public static class CCMapper 
	extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	
	private IntWritable k = new IntWritable();
	private IntWritable v = new IntWritable();

	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {
	    String str = value.toString();
	    String[] data = str.split("\t");
	    k.set(Integer.parseInt(data[0]));
	    v.set(Integer.parseInt(data[1]));
	    context.write(k,v);
	}
    }
  
    public static class CCReducer 
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

	private ArrayList<Integer> node2s = new ArrayList();
	private IntWritable k = new IntWritable();
	private IntWritable v = new IntWritable();
    

	public void reduce(IntWritable key, Iterable<IntWritable> values, 
			   Context context
			   ) throws IOException, InterruptedException {
	    int node1 = key.get();
	    int node2;
	    boolean node1isOn = false;

	    if(node1 == 0) return;

	    node2s.clear();
	    for(IntWritable val : values) {
		node2 = val.get();
		if(node2 == 0) {
		    node1isOn = true;
		} else {
		    node2s.add(node2);
		}
	    }

	    if(node1isOn) {
		v.set(0);
		context.write(key,v);

		for(Integer n2 : node2s) {
		    k.set(n2);
		    context.write(k,v);
		    context.getCounter(CHANGED_NODES_COUNTER.CHANGED_NODES).increment(1);
		}
	    } else {
		for(Integer n2 : node2s) {
		    v.set(n2);
		    context.write(key,v);
		}
	    }
	}
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 2) {
	    System.err.println("Usage: indycascade <in> <out>");
	    System.exit(2);
	}
	Job job = new Job(conf, "indy cascade");
	job.setJarByClass(ConComs.class);
	job.setMapperClass(CCMapper.class);
	job.setCombinerClass(CCReducer.class);
	job.setReducerClass(CCReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);
	TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
	TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	job.waitForCompletion(true);
	Counters counters = job.getCounters();
	Counter c1 = counters.findCounter(CHANGED_NODES_COUNTER.CHANGED_NODES);
	System.out.println(c1.getDisplayName()+":"+c1.getValue());
	System.exit(0);
    }
}
