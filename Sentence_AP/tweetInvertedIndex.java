package Sentence_AP;
import Writables.docLenWritable;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class tweetInvertedIndex{
	
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, docLenWritable>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
		{
			 	String[] Word_ID_Counter = value.toString().split("\t");
		        String[] Word_ID = Word_ID_Counter[0].split("@");
		        String Len=Word_ID_Counter[1].split("/")[1];
		        String word=Word_ID[0];
		        String document=Word_ID[1];
            
		       //output for postings
		       // <WORD, # occurance @ Document>
		       if(!word.isEmpty()) 
		    	   context.write(new Text(word),new docLenWritable(document,Len));
		       //Output [document	word=TF_IDF]	
			
		}
		
		
	}
	
	public static class WordReducer extends Reducer<Text, docLenWritable, Text, Text>{
		
		public void reduce(Text key, Iterable<docLenWritable>values, Context context)throws IOException, InterruptedException
		{
			//one big string to store all the postings
			StringBuilder postings=new StringBuilder();
	        
	        //Create the postings list
	        for (docLenWritable val : values) {
	        	postings.append(val.getDocument()+"LEN"+val.getLength()+"\t");
	        	}
            
	        //write <WORD, posting1	posting2 posting3.......>
	        context.write(key, new Text(postings.toString()));
	       
	    }
    }//end reduce
		
	
	
	public static void findInvertedIndex(String path) throws Exception
	{
		Configuration conf = new Configuration();
	    	    
        Job job = new Job(conf, "Inverted_Index");
        job.setJarByClass(tweetInvertedIndex.class);

	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(docLenWritable.class);
			   
	    
	    //Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);
	    
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/unique");//The input directory path 
	    Path output=new Path(path+"/Postings");//the output path
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}
}