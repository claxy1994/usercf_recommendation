package com.changlinsh.recommendation.usercf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserCF_Step5 {

	public static class Step5_Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = UserCF_DAO.DELIMITER.split(value.toString());
			
			if (tokens.length>=3)
				context.write(new IntWritable(Integer.parseInt(tokens[0])), new Text(tokens[1] + "," + tokens[2]));
		}
		
	}
	
	public static class Step5_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private int RECOMMENDER_NUM;

		@Override
		protected void setup(Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			RECOMMENDER_NUM = UserCF_DAO.readRecommendNumFromHDFS("hdfs://211.87.227.97:9000/user/hadoop/recommendationUtil/recommendNum");
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			Map<Double, String> map = new HashMap<Double, String>();
			
			for (Text value: values) {
				String[] vlist = UserCF_DAO.DELIMITER.split(value.toString());
				
				if (vlist.length>=2)
					map.put(Double.parseDouble(vlist[1]), vlist[0]);
			}
			
			List<Double> list = new ArrayList<Double>();
			Iterator<Double> iter = map.keySet().iterator();
			while(iter.hasNext()) {
				Double similarity = iter.next();
				list.add(similarity);
			}
			
			Collections.sort(list, new Comparator<Double>() {

				@Override
				public int compare(Double similarity1, Double similarity2) {
					return similarity2.compareTo(similarity1);
				}
				
			});
			
			String v = "";
			for (int i=0; i<RECOMMENDER_NUM && i<list.size(); i++) {
				if (list.get(i).compareTo(new Double(0.001))>0)
					v += "," + map.get(list.get(i)) + "[" + String.format("%.2f", list.get(i)) + "]";
			}
			
			if (!v.isEmpty())
				context.write(key, new Text(v.substring(1)));
			else
				context.write(key, new Text("none"));
		}
		
	}

	public int run() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapreduce.job.jar", "F:\\hadoop-eclipse\\mapreduce-jar\\recommendation_usercf_step5.jar");
	    conf.set("fs.defaultFS", "hdfs://211.87.227.97:9000");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "211.87.227.97:8032");
	    conf.set("mapred.child.java.opts", "-Xmx2000m");
	    conf.setBoolean("mapreduce.client.genericoptionsparser.used", true);
	    conf.setInt("mapreduce.tasktracker.map.tasks.maximum", 10);
	    Job job = Job.getInstance(conf, "usercf_step5");
	    job.setJarByClass(UserCF_Step5.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Step5_Mapper.class);
	    job.setReducerClass(Step5_Reducer.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step4_output"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://211.87.227.97:9000/user/hadoop/usercf_step5_output"));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
