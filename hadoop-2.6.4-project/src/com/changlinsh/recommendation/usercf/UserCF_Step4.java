package com.changlinsh.recommendation.usercf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCF_Step4 {
	
	public static class Step4_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private String flag;
		private int itemNum;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit)context.getInputSplit();
			flag = split.getPath().getParent().getName();
			itemNum = UserCF_DAO.readItemNumFromHDFS("hdfs://211.87.227.97:9000/user/hadoop/recommendationUtil/itemNum");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = UserCF_DAO.DELIMITER.split(value.toString());
			
			if (flag.contains("usercf_step3_output")) {
				for (int i=0; i<itemNum; i++) {
					String vStr = tokens[0];
					for (int j=1; j<tokens.length; j+=2)
						vStr += "," + tokens[j];
					
					context.write(new IntWritable(i), new Text("A:" + vStr));
				}
			} else if (flag.contains("input")) {
				
				if (tokens.length>=3)
					context.write(new IntWritable(Integer.parseInt(tokens[1])), new Text("B:" + tokens[0] + "," + tokens[2]));
			}
		}
		
	}
	
	public static class Step4_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Map<String, String> mapA = new HashMap<String, String>();
			Map<String, String> mapB = new HashMap<String, String>();
			
			for (Text value: values) {
				String val = value.toString();
				
				if (val.startsWith("A:")) {
					String[] kv = UserCF_DAO.DELIMITER.split(val.substring(2));
					String vStr = "";
					for (int i=1; i<kv.length; i++)
						vStr += "," + kv[i];
					mapA.put(kv[0], vStr.substring(1));
				}else if (val.startsWith("B:")) {
					String[] kv = UserCF_DAO.DELIMITER.split(val.substring(2));
					mapB.put(kv[0], kv[1]);
				}
			}
			
			Iterator<String>  iterA = mapA.keySet().iterator();
			while (iterA.hasNext()) {
				String userID = iterA.next();
				if (!mapB.containsKey(userID)) {
					String simiStr = mapA.get(userID);
					String[] simi = UserCF_DAO.DELIMITER.split(simiStr);
					double simiVal = 0;
					for (int i=0; i<simi.length; i++)
						simiVal += mapB.containsKey(simi[i]) ? Double.parseDouble(mapB.get(simi[i])) : 0;
					double score = simiVal/simi.length;
					
					context.write(new IntWritable(Integer.parseInt(userID)), new Text(key.toString() + "," + String.format("%.2f", score)));
				}
			}
		}
		
	}

	public int run() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.job.jar", "F:\\hadoop-eclipse\\mapreduce-jar\\recommendation_usercf_step4.jar");
	    conf.set("fs.defaultFS", "hdfs://211.87.227.97:9000");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "211.87.227.97:8032");
	    conf.set("mapred.child.java.opts", "-Xmx2000m");
	    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
	    conf.setInt("mapreduce.tasktracker.map.tasks.maximum", 10);
	    Job job = Job.getInstance(conf, "usercf_step4");
	    job.setJarByClass(UserCF_Step4.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Step4_Mapper.class);
	    job.setReducerClass(Step4_Reducer.class);
	    FileInputFormat.addInputPaths(job, "hdfs://211.87.227.97:9000/user/hadoop/usercf_step3_output,hdfs://211.87.227.97:9000/user/hadoop/input");
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step4_output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
