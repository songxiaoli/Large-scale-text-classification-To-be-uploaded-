package Sentence_AP;
import Writables.similarityVectorWritable;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class twitterSimilarity{

	public static class WordMapper extends Mapper<LongWritable, Text, IntWritable, similarityVectorWritable>{
		// <term  documentIDLENlength>  -- >
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			String[]Line=value.toString().split("\t");
		
		    ArrayList<String>Doc_ID=new ArrayList<String>();
		    ArrayList<String>docLength=new ArrayList<String>();
   
		     for(int i=1; i<Line.length; i++){
			     String[]Count_Doc=Line[i].split("LEN");
			     docLength.add(Count_Doc[1]);
			     Doc_ID.add(Count_Doc[0]); 
		     }
		    
		     for(int i=0; i<Doc_ID.size(); i++){
		    	 for(int j=0; j<Doc_ID.size(); j++){
		    		 String vector1=docLength.get(i);
		    		 String vector2=docLength.get(j);
		    		 context.write(new IntWritable(Integer.parseInt(Doc_ID.get(i))), new similarityVectorWritable(Doc_ID.get(j),vector1,vector2));
		    	 }
		     }	
		}	
	}
	
	public static class WordReducer extends Reducer<IntWritable, similarityVectorWritable, Text, Text>{
		public void reduce(IntWritable key, Iterable<similarityVectorWritable>values, Context context)throws IOException, InterruptedException
		{
			//new
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
			int numOfWordInDict = Integer.parseInt(conf.get("numOfWordInDict"));
			int preference = Integer.parseInt(conf.get("preference"));
		    //new
			int doc1 = key.get();
			HashMap<Integer, String> mapping = new HashMap<Integer,String>();
		
			double similarity=0;
			int NumberOfWordsInDoc1 = 0;
			for(similarityVectorWritable val:values){
				int doc2 = val.getDoc2();
				NumberOfWordsInDoc1 = (int) val.getVector1();
				
				if(mapping.containsKey(doc2)){
					String tmp = mapping.get(doc2)+";"+(int)val.getVector1()+","+(int)val.getVector2();
					mapping.put(doc2, tmp);
				}else
                    mapping.put(doc2, (int)val.getVector1()+","+(int)val.getVector2());
			}
			
			for(int noDoc2 = 1; noDoc2 < numOfDocs+1; noDoc2++){
				if(mapping.containsKey(noDoc2)){
					if(doc1 == noDoc2){
						int NumberOfWordsInDoc = Integer.parseInt(mapping.get(noDoc2).split(";")[0].split(",")[0]);
						similarity = -Math.log10(numOfWordInDict)*NumberOfWordsInDoc-preference;
					}else{
						int commonWords = mapping.get(noDoc2).split(";").length;				
						NumberOfWordsInDoc1 = Integer.parseInt(mapping.get(noDoc2).split(";")[0].split(",")[0]);
						int NumberOfWordsInDoc2 = Integer.parseInt(mapping.get(noDoc2).split(";")[0].split(",")[1]);
						int uncommonWords=NumberOfWordsInDoc1 - commonWords;
						similarity=-commonWords*Math.log10(NumberOfWordsInDoc2)+(-uncommonWords*Math.log10(numOfWordInDict));
					}
				}else{					
					similarity = -Math.log10(numOfWordInDict)*NumberOfWordsInDoc1;
				}
				 context.write(new Text(doc1+" and "+noDoc2),new Text(String.valueOf(similarity)+",0,0")); //key: <doc1 and doc2>
				// value: <sim,a,r> 
			}			
	    }	
	}
		
	
	
	public static void findSimilarity(String path, String numOfDocs, String numOfWordsInDicts, String preference)throws Exception
	{
		
		Configuration conf = new Configuration();
		//new: pass the parameter(number of documents) to the mapper and reducer;
	    conf.set("numOfDocs",numOfDocs);    
	    conf.set("numOfWordInDict", numOfWordsInDicts);
	    conf.set("preference", preference);
	    //new
	    Job job = new Job(conf, "Pairwise_Similarity");
	    
	    job.setJarByClass(twitterSimilarity.class);
	    
	    job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(similarityVectorWritable.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Postings");
	    Path output=new Path(path+"/Similarity");
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}
	
	
	
	
	
}