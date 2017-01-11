package Cosine_AP;

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


public class Cosine_Similarity_complementary{
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
		{
			String[]line = value.toString().split("\t");
			String[]documents = line[0].split(" and ");
			System.out.println("jaja "+documents[0]+" "+documents[1]+";"+line[1]); 
			context.write(new Text(documents[0]), new Text(documents[1]+";"+line[1]));
		}
	}
    
	public static class WordReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text>values, Context context)throws IOException, InterruptedException{

			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
			int preference = Integer.parseInt(conf.get("preference"));
			HashMap<Integer, String> docAndSim = new HashMap<Integer,String>();
			
			int doc1ID = Integer.parseInt(key.toString());
			for(Text val: values){
				int doc2ID = Integer.parseInt(val.toString().split(";")[0]);
				String sim = val.toString().split(";")[1];
				docAndSim.put(doc2ID, sim);
			}
			for(int noOfDoc2 = 1; noOfDoc2 < numOfDocs+1; noOfDoc2++){
				if(noOfDoc2 == doc1ID){
					context.write(new Text(noOfDoc2+" and "+noOfDoc2), new Text(Integer.toString(1-preference)+",0,0"));
				    continue;
				}
				if(docAndSim.containsKey(noOfDoc2)){
					context.write(new Text(doc1ID+" and "+noOfDoc2), new Text(docAndSim.get(noOfDoc2)));
				    continue;
				}
				context.write(new Text(doc1ID+" and "+noOfDoc2), new Text("0,0,0"));
			}
		}

	}//end reduce

	public static void Cosine_Similarity_Complementary(String path, String numOfDocs, String preference)throws Exception{		

		Configuration conf = new Configuration();
		conf.set("numOfDocs", numOfDocs);
		conf.set("preference", preference);

		Job job = new Job(conf, "Cosine_Similarity_complementary");
		job.setJarByClass(Cosine_Similarity_complementary.class);

		//Outputting key value pairs as a dictionary (rememb python)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//Setting the mapper and reducer classes
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		//Setting the type of input format. In this case, plain TEXT
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path input=new Path(path+"/pre_Similarity");//The input directory path 
		Path output=new Path(path+"/Similarity");//the output path

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(output)) {
			fs.delete(output, true);
		}

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}	
}