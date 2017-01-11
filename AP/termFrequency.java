package AP;

import Writables.wordCountWritable;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class termFrequency{
	public static class WordMapper extends Mapper<LongWritable, Text, IntWritable,wordCountWritable>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			 String[] Word_ID_Counter = value.toString().split("\t");
		        String[] TweetID = Word_ID_Counter[0].split("INDOCUMENT");
		       context.write(new IntWritable(Integer.parseInt(TweetID[1])), new wordCountWritable(TweetID[0],Word_ID_Counter[1]));
		       //Output [TweetID	word=wordCount]
		}
	}
	
	public static class WordReducer extends Reducer<IntWritable, wordCountWritable, Text, Text>{
		public void reduce(IntWritable key, Iterable<wordCountWritable>values, Context context)throws IOException, InterruptedException{
			int Sum = 0;//N (sum of words in manuscript dictionary)
	        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
	        
	        //loop through values and get word occurance and sum of words
	        for (wordCountWritable val : values) {
	            tempCounter.put(val.getWord(), val.getCount());
	            Sum+=Integer.valueOf(val.getCount());
	        }
	        
	        //loop through the map to output reducer job
	        for(String word: tempCounter.keySet()){
	        	String count=String.valueOf(tempCounter.get(word));//get the count
	        	context.write(new Text(word+"@"+key.toString()), new Text(count+"/"+Sum));
	        }
	       
	    }
    }//end reduce
		
	public static void findTF(String path) throws Exception{
		Configuration conf = new Configuration();
	    	    
        Job job = new Job(conf, "TermInTweets");
        job.setJarByClass(termFrequency.class);
	    
	       
	    job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(wordCountWritable.class);
			
	    //Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);
	    
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/terms");//The input directory path 
	    Path output=new Path(path+"/unique");//the output path
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}	
}