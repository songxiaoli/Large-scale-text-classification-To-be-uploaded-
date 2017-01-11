package AP;

import Writables.document_data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class responsibility {
	public static class ResponsibilityMap extends Mapper<LongWritable, Text, Text, document_data>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			
			/*
			 * Input <doc1 and doc2>, <sim, a, r>
			 * Output<doc1>, <doc2, sim, a, r, true/false> 
			 */
			String[]line = value.toString().split("\t");
			String[]s_r_a = line[1].split(",");
			String[]documents = line[0].split(" and ");
			
			String doc1 = documents[0];
			String doc2 = documents[1];
			String sim = s_r_a[0];
			String resp = s_r_a[1];
			String avail = s_r_a[2];
	        document_data outputMap = new document_data(doc2,sim,resp,avail);
	        context.write(new Text(doc1), outputMap);
		}		
	}//end class
	
	public static class ResponsibilityReduce extends Reducer<Text, document_data, Text, Text>{
		
		public void reduce(Text key, Iterable<document_data>values, Context context) throws IOException, InterruptedException{			
			   
            Configuration conf = context.getConfiguration();
            int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));
            //new
			
            Map<String,String> responsibility = new HashMap<String, String>();
            String doc1 = key.toString();
            HashMap<Integer,document_data> list_s_r_a = new HashMap<Integer,document_data>() ;
            
            for(document_data val:values){
                list_s_r_a.put(val.getDoc2(),new document_data(val.getDoc2(), val.getSimilarity(), val.getResponsibility(), val.getAvailibility()));
            }
            // r(i,k) <---- s(i,k) - max {a(i,k')+s(i,k')}
            // loop through all doc2s to calculate the r(doc1,doc2)
            //new: get the parameter;
            //new

            for(int noDoc2 = 1; noDoc2 < numOfDocs+1; noDoc2++){
                Double newResponsibility = 0.0;
                if(noDoc2 == Integer.parseInt(doc1)){
                    double selfSimilarity = list_s_r_a.get(noDoc2).getSimilarity();
                    ArrayList<Double> FindMax = new ArrayList<Double>();
                    for(int each = 1; each < numOfDocs+1; each++){
                        if(each == noDoc2){
                            continue;
                        }
                        FindMax.add((double)list_s_r_a.get(each).getSimilarity());
                    }
                    newResponsibility = selfSimilarity-Collections.max(FindMax);
                }else{
                    double similarity_doc1_doc2 = (double) list_s_r_a.get(noDoc2).getSimilarity();
                    ArrayList<Double> FindMax = new ArrayList<Double>();
                    for(int each = 1; each < numOfDocs+1; each++){
                        if(each == noDoc2){
                            continue;
                        }
                        FindMax.add((double)list_s_r_a.get(each).getSimilarity()+list_s_r_a.get(each).getAvailibility());
                    }
                    newResponsibility = similarity_doc1_doc2-Collections.max(FindMax);
                }
                Double oldResponsibility = (double) list_s_r_a.get(noDoc2).getResponsibility();
                responsibility.put(Integer.toString(noDoc2), Double.toString(0.5*oldResponsibility +0.5*newResponsibility));
            }
            
				//output all the responsibility
            for(int doc2 = 1; doc2 < numOfDocs+1; doc2++){
                context.write(new Text(doc1+" and "+doc2), new Text(list_s_r_a.get(doc2).getSimilarity()+","+responsibility.get(Integer.toString(doc2))+","+list_s_r_a.get(doc2).getAvailibility()));
            }
        }
    }//end class

	public static void updateResponsibility(String path, String numOfDocs,int Reducers) throws Exception{
		Configuration conf= new Configuration();
		//new: pass the parameter(number of documents) to the mapper and reducer;
	    conf.set("numOfDocs",numOfDocs);    
	    
	    //new
		Job job = new Job(conf);
		job.setNumReduceTasks(Reducers);//SETTING # OF REDUCERS
		job.setJarByClass(responsibility.class);
			job.setJobName("Responsibility");
			 //Outputting key value pairs as a dictionary (rememb python)
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(document_data.class);
		    
		    //Setting the mapper and reducer classes
		    job.setMapperClass(ResponsibilityMap.class);
		    job.setReducerClass(ResponsibilityReduce.class);
		    
		    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    Path input=new Path(path+"/Similarity");//The input directory path 
	    Path output=new Path(path+"/iteration_output");//the output path
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	        
	    job.waitForCompletion(true);	
	}	
}