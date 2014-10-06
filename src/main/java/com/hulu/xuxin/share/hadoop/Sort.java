package com.hulu.xuxin.share.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sort {

	public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable();
		
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
				int i = Integer.parseInt(value.toString());
				data.set(i);
				context.write(data, new IntWritable(1));
		}
	}
	
	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable linenum = new IntWritable(1);
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get() + 1);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: sort <in> <out>");
			System.exit(2);
		}
		Util.removeExistingPath(args[1]);

		Job job = new Job(configuration, "sort");
		job.setJarByClass(Sort.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
