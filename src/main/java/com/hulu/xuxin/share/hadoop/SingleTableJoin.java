package com.hulu.xuxin.share.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SingleTableJoin {

	public static int time = 0;
	
	public static class STMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String childName = new String();
			String parentName = new String();
			String relationType = new String();
			String line = value.toString();
			
			String[] values = line.split("[ ]+");
			if (values[0].compareTo("child") != 0) {
				childName = values[0];
				parentName = values[1];
				relationType = "1";
				context.write(new Text(values[1]), new Text(relationType + "+" + childName + "+" + parentName));
				relationType = "2";
				context.write(new Text(values[0]), new Text(relationType + "+" + childName + "+" + parentName));
			}
		}
	}
	
	public static class STReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			if (time == 0) {
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			ArrayList<String> grandchild = new ArrayList<String>();
			ArrayList<String> grandparent = new ArrayList<String>();
			Iterator iterator = values.iterator();
			while (iterator.hasNext()) {
				String record = iterator.next().toString();
				int len = record.length();
				if (len == 0) continue;
				char relationType = record.charAt(0);
				
				String []contents = record.split("\\+");
				String childName = contents[1];
				String parentName = contents[2];
				if (relationType == '1')
					grandchild.add(childName);
				else
					grandparent.add(parentName);
			}
			
			for (int i = 0; i < grandchild.size(); i++) {
				for (int j = 0; j < grandparent.size(); j++) {
					context.write(new Text(grandchild.get(i)), new Text(grandparent.get(j)));
				}
			}
			
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: STJoin <in> <out>");
			System.exit(2);
		}

		Job job = new Job(configuration, "STJoin");
		job.setJarByClass(SingleTableJoin.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(STMapper.class);
		job.setReducerClass(STReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}
