package AP;

import Writables.document_data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

public class availibility {
	public static class AvailibilityMap extends Mapper<LongWritable, Text, Text, document_data>{
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
			
	        document_data outputMap = new document_data(doc1,sim,resp,avail);
			context.write(new Text(doc2),outputMap);
		}		
	}
	
	/**
	 * Runs availibility calculation
	 *
	 */
	public static class AvailibilityReduce extends Reducer<Text, document_data, Text, Text>{

		
		public void reduce(Text key, Iterable<document_data>values, Context context) throws IOException, InterruptedException{
			//new: get the parameter;
			Configuration conf = context.getConfiguration();
			int numOfDocs = Integer.parseInt(conf.get("numOfDocs"));

			//new
			
			//compute availibility
			Map<String,String> availibility = new HashMap<String, String>();	
			String doc2 = key.toString(); //this is k not i. //DOCUMENT ID
			HashMap<Integer,document_data> list_s_r_a = new HashMap<Integer,document_data>();

			for(document_data val:values){
				int doc1 = val.getDoc2();
				list_s_r_a.put(doc1,new document_data(Integer.parseInt(doc2),val.getSimilarity(),val.getResponsibility(), val.getAvailibility()));	
			}
			//a(i,k)<----- min{0,r(k,k) + SUM(MAX(0,r(i',k))}

			for(int noDoc1 = 1; noDoc1 < numOfDocs+1; noDoc1++){			
				Double newAvailibility = 0.0;
				if(noDoc1 == Integer.parseInt(doc2)){
					Double sumResp = 0.0;
					for(int each = 1; each < numOfDocs+1; each++){
						if(each == noDoc1)
							continue;
						if(list_s_r_a.get(each).getResponsibility() > 0.0)
							sumResp += list_s_r_a.get(each).getResponsibility();
						
					}
					newAvailibility = sumResp;
					
				}else{
					Double selfResponsibility = (double)list_s_r_a.get(Integer.parseInt(doc2)).getResponsibility();
					Double sumResp = 0.0;
					for(int each = 1; each < numOfDocs+1; each++){
						if( each == noDoc1 | each == Integer.parseInt(doc2))
							continue;
						if(list_s_r_a.get(each).getResponsibility() > 0.0)
							sumResp += list_s_r_a.get(each).getResponsibility();
					}
					if( selfResponsibility + sumResp < 0.0)
						newAvailibility = selfResponsibility + sumResp;
					
					else
						newAvailibility = 0.0;
				}
				double oldAvailbility = list_s_r_a.get(noDoc1).getAvailibility();
				
				double avail = 0.5*oldAvailbility+0.5*newAvailibility;
				
				availibility.put(Integer.toString(noDoc1), Double.toString(avail));
			}
			
			//OUTPUT AVAILIBILITY
			for(int doc1 = 1; doc1 < numOfDocs+1; doc1++){		
				String avail = availibility.get(Integer.toString(doc1));
				context.write(new Text(doc1+" and "+doc2), new Text(list_s_r_a.get(doc1).getSimilarity()+","+list_s_r_a.get(doc1).getResponsibility()+","+avail)); 
			}					
        }//end method				
	}

    public static void updateAvailibility(String path,String numOfDocs,int Reducers) throws Exception{
        Configuration conf= new Configuration();
			
        conf.set("numOfDocs",numOfDocs);
        //new
        Job job = new Job(conf);
        job.setJarByClass(availibility.class);
        job.setJobName("Availibitily and Responsibility");
        job.setNumReduceTasks(Reducers);
        
        //Outputting key value pairs as a dictionary (rememb python)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(document_data.class);
		    
        //Setting the mapper and reducer classes
        job.setMapperClass(AvailibilityMap.class);
        job.setReducerClass(AvailibilityReduce.class);
        
        //Setting the type of input format. In this case, plain TEXT
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path input=new Path(path+"/iteration_output");//The input directory path
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





