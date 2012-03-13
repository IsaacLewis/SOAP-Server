package me.saac.i;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IndyCascade {

  public static class ICMapper 
      extends Mapper<Object, Text, IntWritable, IntWritable>{
    
    private IntWritable k = new IntWritable();
    private IntWritable v = new IntWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String str = value.toString();
	Text t = new Text(str);
	String[] data = str.split(" ");
	int node1;
	int node2;

	// if line represents an edge
	if(Character.isDigit(str.charAt(str.length()-1))) {
	    node1 = Integer.parseInt(data[0]);
	    node2 = Integer.parseInt(data[1]);
	    double weight = Float.parseFloat(data[2]);

	    // roll a dice to see if it should remain
	    if(Math.random() < weight) {
		// text = new Text(str + " open e-dge");
		k.set(node1);
		v.set(node2);
		context.write(k, v);
		context.write(v, k);
	    } else {
		// text = new Text(str + " closed edge");
	    }

	} else {
	    if(data[2].equals("true")) {
		node1 = Integer.parseInt(data[0]);
		k.set(node1);
		v.set(0);
		context.write(k,v);
	    }
	}
    }
  }
  
  public static class ICReducer 
      extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
	for(IntWritable val : values) {
	    context.write(key, val);
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
    job.setJarByClass(IndyCascade.class);
    job.setMapperClass(ICMapper.class);
    job.setCombinerClass(ICReducer.class);
    job.setReducerClass(ICReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
