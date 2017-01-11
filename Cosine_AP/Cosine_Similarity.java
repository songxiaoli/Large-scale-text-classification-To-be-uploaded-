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

public class Cosine_Similarity{
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String[]Line=value.toString().split("\t");
		    ArrayList<String>Doc_ID=new ArrayList<String>();
		    ArrayList<Double>TF_IDF=new ArrayList<Double>();
		    
		    //store the doc ID and the counts in arraylist
		     for(int i=1; i<Line.length; i++){
			     String[]Count_Doc=Line[i].split("@");
			     TF_IDF.add(Double.parseDouble(Count_Doc[0]));//store Weight value
			     Doc_ID.add(Count_Doc[1]);//store ID
			     System.out.println("docID: "+Count_Doc[1]);
		     }

		     //compare documents
		     for(int i=0; i<Doc_ID.size(); i++){//for each document
		    	 for(int j=i+1; j<Doc_ID.size(); j++){//loop through other documents
		    		 //add scores for each document together 
		    		 String vector1=TF_IDF.get(i).toString();
		    		 String vector2=TF_IDF.get(j).toString();
		    		 System.out.println(Doc_ID.get(i)+" and "+Doc_ID.get(j));
		    		 context.write(new Text(Doc_ID.get(i)+" and "+Doc_ID.get(j)), new Text(vector1+","+vector2));
		    		 //<Pre condition
		    		 //Word d1 d2 d3...... postings
		    		 //POST_Conditions
		    		 //Mapper Output=> <Doc1 and Doc2>, <vector1, vector2>
		    	 }
		     }
		}
	}
	
	public static class WordReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text>values, Context context)throws IOException, InterruptedException{
            
            float CrossProduct=0;
            float d1_SquaredDistance=0;
            float d2_SquaredDistance=0;
            for(Text val: values){
                //split vector1,vector2
                String[]vectors=val.toString().split(",");
					
                float vector1=Float.parseFloat(vectors[0]);
                float vector2=Float.parseFloat(vectors[1]);
                CrossProduct+=(vector1*vector2);//increment the cross produce v1*v2
					
                //increment each squared distance for square root later
                d1_SquaredDistance+=vector1*vector1;
                d2_SquaredDistance+=vector2*vector2;
            }
			   
            double Denominator=(Math.sqrt(d1_SquaredDistance) * Math.sqrt(d2_SquaredDistance));
            double CosineSimilarity= CrossProduct/Denominator;
            int doc1ID = Integer.parseInt(key.toString().split(" and ")[0]);
            int doc2ID = Integer.parseInt(key.toString().split(" and ")[1]);
			   
            context.write(new Text(doc1ID+" and "+doc2ID), new Text(String.valueOf(CosineSimilarity)+",0,0"));
            context.write(new Text(doc2ID+" and "+doc1ID), new Text(String.valueOf(CosineSimilarity)+",0,0"));
            //print out the availibility and responsibility as well.
        }
    }//end reduce
	
	public static void Find_Cosine_Similarity(String path)throws Exception{

		Configuration conf = new Configuration();
	    Job job = new Job(conf, "Cosine_Similarity");
	    job.setJarByClass(Cosine_Similarity.class);

	    //Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(WordMapper.class);
	    job.setReducerClass(WordReducer.class);
	    
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Postings");//The input directory path 
	    Path output=new Path(path+"/pre_Similarity");//the output path
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);
	}	
}