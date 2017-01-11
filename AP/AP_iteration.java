package AP;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sun.awt.windows.ThemeReader;

public class AP_iteration {
	public static void iterate (String path, String numOfDocs, double THREASHOLD,int Reducers) throws Exception {
		int sameCount = 0;
		int countFile = 0;
		boolean accumulatedSameFlag = false;			
		responsibility_initial.initializeResponsibility(path,numOfDocs);
		int numOfIteration = 0;
		double oldSim[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double oldResp[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double oldAvail[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newSim[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newResp[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		double newAvail[][] = new double[Integer.parseInt(numOfDocs)][Integer.parseInt(numOfDocs)];
		for(int noOfDoc1 = 0; noOfDoc1 < Integer.parseInt(numOfDocs); noOfDoc1++){
			for(int noOfDoc2 = 0; noOfDoc2 < Integer.parseInt(numOfDocs); noOfDoc2++){
				oldSim[noOfDoc1][noOfDoc2] = 0.0;
				oldResp[noOfDoc1][noOfDoc2] = 0.0;
				oldAvail[noOfDoc1][noOfDoc2] = 0.0;
			}
		}
	    //Iterate until converges or after 100 iterations.
		for(int i=0; i < 100 ;i++){		
			System.out.println("This is the "+i+"th iteration"+" samecount:"+sameCount);
			availibility.updateAvailibility(path,numOfDocs,Reducers);			
			Configuration conf = new Configuration();
			
			//The following is configured to read from the hdfs system instead of the local disk; 
			//the path is the directory for the hadoop configuration on lincs;
			
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
			
			FileSystem fileSystem = FileSystem.get(conf);
			Path filePath = new Path(path+"/Similarity/part-r-00000");
			FSDataInputStream br = fileSystem.open(filePath);
			String strLine = "";
			while((strLine = br.readLine())!= null){
		    	String line[] = strLine.split("\t");
		    	int row = Integer.parseInt(line[0].split(" and ")[0]);
		    	int colum = Integer.parseInt(line[0].split(" and ")[1]);
		    	
		    	newSim[row-1][colum-1] = Double.parseDouble(line[1].split(",")[0]);
		    	newResp[row-1][colum-1] = Double.parseDouble(line[1].split(",")[1]);
		    	newAvail[row-1][colum-1] = Double.parseDouble(line[1].split(",")[2]);
		    }
			
		    boolean sameFlag = true;
		    for(int noOfDoc1 = 0; noOfDoc1 < Integer.parseInt(numOfDocs); noOfDoc1++){
		    	for(int noOfDoc2 = 0; noOfDoc2 < Integer.parseInt(numOfDocs); noOfDoc2++){
		    		if(Math.abs((newAvail[noOfDoc1][noOfDoc2]+newResp[noOfDoc1][noOfDoc2])-(oldAvail[noOfDoc1][noOfDoc2]+oldResp[noOfDoc1][noOfDoc2])) > THREASHOLD) 
		    			sameFlag=false;
			    	}
			     }
		    if(sameFlag == true && accumulatedSameFlag == false){
		    	sameCount = 1; 
		    	accumulatedSameFlag = true;
		    }
		    if(sameFlag == true && accumulatedSameFlag == true){
		    	sameCount ++;
		    }
		    if(sameFlag == false  && accumulatedSameFlag == true ){
		    	accumulatedSameFlag = false;
		    	sameCount = 0;
		    }
		    //if the evidence doesn't change for 10 iterations, break the loop.
		    if(sameCount == 10) {
		    	System.out.println("Iteration times: "+i);
		    	numOfIteration = i;
		    	break;
		    }
		    for(int noOfDoc1 = 0; noOfDoc1 < Integer.parseInt(numOfDocs); noOfDoc1++){
		    	for(int noOfDoc2 = 0; noOfDoc2 < Integer.parseInt(numOfDocs); noOfDoc2++){
		    		oldSim[noOfDoc1][noOfDoc2] = newSim[noOfDoc1][noOfDoc2];
		    		oldResp[noOfDoc1][noOfDoc2] = newResp[noOfDoc1][noOfDoc2];
		    		oldAvail[noOfDoc1][noOfDoc2] = newAvail[noOfDoc1][noOfDoc2];
		    	}
		    } 

		    responsibility.updateResponsibility(path,numOfDocs,Reducers);
		}
		if( numOfIteration == 99 ){
			availibility.updateAvailibility(path,numOfDocs,Reducers);
		}
	}
}