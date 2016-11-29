package com.changlinsh.recommendation.usercf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCF_Step2 {

	public static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = UserCF_DAO.DELIMITER.split(value.toString());
			
			if (tokens.length>=4)
				context.write(new Text(tokens[0] + "," + tokens[1]), new Text(tokens[2] + "," + tokens[3]));
		}
		
	}
	
	public static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			double similarity = 0.0;
			int num = 0;
			
			for (Text value: values) {
				String[] vlist = UserCF_DAO.DELIMITER.split(value.toString());
				
				if (vlist.length>=2) {
					sum += Math.pow(Double.parseDouble(vlist[0])-Double.parseDouble(vlist[1]), 2);
					num += 1;
				}
			}
			
			if (sum > 0.00000001)
				similarity = (double)num/(1+Math.sqrt(sum));
			
			context.write(key, new Text(String.format("%.7f", similarity)));
		}
		
	}

	public int run() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.job.jar", "F:\\hadoop-eclipse\\mapreduce-jar\\recommendation_usercf_step2.jar");
	    conf.set("fs.defaultFS", "hdfs://211.87.227.97:9000");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "211.87.227.97:8032");
	    conf.set("mapred.child.java.opts", "-Xmx2000m");
	    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
	    conf.setInt("mapreduce.tasktracker.map.tasks.maximum", 10);
	    Job job = Job.getInstance(conf, "usercf_step2");
	    job.setJarByClass(UserCF_Step2.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Step2_Mapper.class);
	    job.setReducerClass(Step2_Reducer.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step1_output"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step2_output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
