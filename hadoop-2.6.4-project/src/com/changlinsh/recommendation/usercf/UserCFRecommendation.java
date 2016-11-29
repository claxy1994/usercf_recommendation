package com.changlinsh.recommendation.usercf;

public class UserCFRecommendation {
	
	public static void main(String[] args) throws Exception {
		run(10, 300, "hdfs://211.87.227.97:9000/user/hadoop/recommendationUtil/");
		
		System.exit(0);
	}
	
	public static void run(int neighborNum, int recommendNum, String src) throws Exception {
		UserCF_DAO.writeNeighborNumToHDFS(neighborNum, src + "neighborNum");
		UserCF_DAO.writeRecommendNumToHDFS(recommendNum, src + "recommendNum");
		
		new UserCF_Step1().run();
		new UserCF_Step2().run();
		new UserCF_Step3().run();
		new UserCF_Step4().run();
		new UserCF_Step5().run();
	}
	
}
