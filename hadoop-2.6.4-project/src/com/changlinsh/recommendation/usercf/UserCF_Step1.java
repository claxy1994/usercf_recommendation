package com.changlinsh.recommendation.usercf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCF_Step1 {
	
	public static class Step1_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = UserCF_DAO.DELIMITER.split(value.toString());
			
			if (tokens.length>=3) {
				context.write(new Text(tokens[1]), new Text(tokens[0] + "," + tokens[2]));
			}
		}
		
	}
	
	public static class Step1_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String, String> map = new HashMap<String, String>();
			
			context.getCounter("ItemCounter", "totalItem").increment(1);

			for (Text value: values) {
				String[] vlist = UserCF_DAO.DELIMITER.split(value.toString());
				
				if (vlist.length>=2)
					map.put(vlist[0], vlist[1]);
			}
			
			Iterator<String> iterA = map.keySet().iterator();
			while (iterA.hasNext()) {
				String k1 = iterA.next();
				String v1 = map.get(k1);
				
				Iterator<String> iterB = map.keySet().iterator();
				while (iterB.hasNext()) {
					String k2 = iterB.next();
					String v2 = map.get(k2);
					
					context.write(new Text(k1 + "," + k2), new Text(v1 + "," + v2));
				}
			}
		}
		
	}

	public int run() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.job.jar", "F:\\hadoop-eclipse\\mapreduce-jar\\recommendation_usercf_step1.jar");
	    conf.set("fs.defaultFS", "hdfs://211.87.227.97:9000");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "211.87.227.97:8032");
	    conf.set("mapred.child.java.opts", "-Xmx2000m");
	    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
	    conf.setInt("mapreduce.tasktracker.map.tasks.maximum", 10);
	    Job job = Job.getInstance(conf, "usercf_step1");
	    job.setJarByClass(UserCF_Step1.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Step1_Mapper.class);
	    job.setReducerClass(Step1_Reducer.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/input"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step1_output"));
	    boolean success = job.waitForCompletion(true);
	    if (success) {
	    	int itemNum = (int)job.getCounters().findCounter("ItemCounter", "totalItem").getValue();
	    	UserCF_DAO.writeItemNumToHDFS(itemNum, "hdfs://211.87.227.97:9000/user/hadoop/recommendationUtil/itemNum");
	    	return itemNum;
	    } else return -1;
	}

}
