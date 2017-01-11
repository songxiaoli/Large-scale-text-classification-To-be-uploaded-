package AP;

import Cosine_AP.Cosine_Inverted_Index;
import Cosine_AP.Cosine_Similarity;
import Cosine_AP.Cosine_Similarity_complementary;
import Cosine_AP.TF_IDF;
import Sentence_AP.*;

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
	
public class Affinity_Propagation{
	public static void main(String[] args) throws Exception{
		int Num_Reducers;
		//QUIT TTHE PROGRAM AND PRINT A USAGE MESSAGE
		if(args.length < 6){
			System.out.println("Error! Please" +
					"Enter Arguments as Specified:\n\npath " +
					"fileName\nnumOfDocs\nnumOfWordInDoc\nchoice\npreference\nTHREASHOLD\nNumberofReducers(Optional) ");
		System.exit(1); // QUIT
		}
		//IF REDUCERS NOT SPECIFIED THEN JUST SET TO 1
		if(args.length==7){
			Num_Reducers=1;
		}
		else{
			Num_Reducers=Integer.parseInt(args[7]);
		}
		
		String path = args[0];
		String fileName = args[1];
		String numOfDocs = args[2];
		String numOfWordInDoc = args[3];
	    String choice = args[4];
		String preference = args[5];
		double THREASHOLD = Double.parseDouble(args[6]);
		
		//FIRST DO WORDCOUNT
		wordCount.wordCount(path,fileName);
		//THEN FIND n/N
		termFrequency.findTF(path);
		//THEN INVERTED INDEX
		
		//THEN CALCULATE SIMILARITY
		if(choice == "S"){
			tweetInvertedIndex.findInvertedIndex(path);
			twitterSimilarity.findSimilarity(path,numOfDocs,numOfWordInDoc,preference);
		}else{
			TF_IDF.Find_TF_IDF(path);
			Cosine_Inverted_Index.Find_Cosine_Index(path);
			Cosine_Similarity.Find_Cosine_Similarity(path);
			Cosine_Similarity_complementary.Cosine_Similarity_Complementary(path,numOfDocs,preference);
		}
		
		//COMPUTE ELAPSED TIME
		long start = System.currentTimeMillis();
		
		AP_iteration.iterate(path, numOfDocs, THREASHOLD,Num_Reducers);
		//do n iterations of responsibility and availibility
		findExemplar.exemplar_finder(path);
		findCluster.cluster_finder(path);
		show_result.result(path);
		
		long ElapsedTime=System.currentTimeMillis()-start; //END TIME
		float elapsedTimeSec = ElapsedTime/1000F; //GET THE TIME IN SECONDS
	
		System.out.println("\n\n\n\n");
		System.out.println("ALGORITHM RUNTIME:"+elapsedTimeSec+" seconds For Input Size "+numOfDocs);
		System.out.println("\n With "+Num_Reducers+" Reducers for A and R");
		
		
		
	}
}