package com.changlinsh.recommendation.usercf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class UserCF_DAO {
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	public static void writeItemNumToHDFS(int itemNum, String src) throws IOException {
		writeDataToHDFS(itemNum, src);
	}
	
	public static int readItemNumFromHDFS(String src) throws IOException {
		int itemNum = readDataFromHDFS(src);
		return itemNum;
	}
	
	public static void writeNeighborNumToHDFS(int neighborNum, String src) throws IOException {
		writeDataToHDFS(neighborNum, src);
	}
	
	public static int readNeighborNumFromHDFS(String src) throws IOException {
		int neighborNum = readDataFromHDFS(src);
		return neighborNum;
	}
	
	public static void writeRecommendNumToHDFS(int recommendNum, String src) throws IOException {
		writeDataToHDFS(recommendNum, src);
	}
	
	public static int readRecommendNumFromHDFS(String src) throws IOException {
		int recommendNum = readDataFromHDFS(src);
		return recommendNum;
	}
	
	public static void writeDataToHDFS(int data, String src) throws IOException {
		InputStream in = new ByteArrayInputStream(Integer.toString(data).getBytes());
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(src), conf);
		OutputStream out = fs.create(new Path(src));
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	
	public static int readDataFromHDFS(String src) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(src));
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		IOUtils.copyBytes(in, out, 4096, true);
		byte[] buff = out.toByteArray();
		return Integer.parseInt(new String(buff));
	}
}
