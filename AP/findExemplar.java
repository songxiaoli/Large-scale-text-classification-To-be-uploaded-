package AP;
import Writables.document_data;
import AP.findExemplar;

import java.io.IOException;
import java.util.HashMap;

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

public class findExemplar{
    public static class ExampleMapper extends Mapper<LongWritable, Text, Text, Text>{
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
            document_data outputMap2 = new document_data(doc1,sim,resp,avail);

            // if doc1 is equal to doc2, then check if it is an exemplar. If yes, emit the output <doc1> <doc2 sim>;
            // else emit the output<doc1> <doc2 sim>;
            if(doc1.equals(doc2)){
                if((Double.parseDouble(resp)+Double.parseDouble(avail)) > 0.0)
                    context.write(new Text(doc1), new Text(outputMap.getDoc2()+","+outputMap.getSimilarity()));
            }else{
                context.write(new Text(doc2), new Text(outputMap2.getDoc2()+","+outputMap2.getSimilarity()));
            }
        }
    }

	public static class ExampleReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
			HashMap<Integer,Double> list_s_r_a = new HashMap<Integer,Double>();
			int doc1 = Integer.parseInt(key.toString()); //this is k not i. 
		//put all the document data into the data structure
			for(Text val:values){
				String[]data = val.toString().split(",");
				String doc2 = data[0];
				String sim = data[1];
				list_s_r_a.put(Integer.parseInt(doc2),Double.parseDouble(sim));
			}

			if(list_s_r_a.containsKey(doc1)){
				for(int noDoc2 = 1; noDoc2 < list_s_r_a.size(); noDoc2++ ){		
					context.write(new Text(Integer.toString(noDoc2)), new Text(Integer.toString(doc1)+" "+Double.toString(list_s_r_a.get(noDoc2))));
				}
			}	
		}//end method	
	}	

    public static void exemplar_finder(String path) throws Exception{
		Configuration conf= new Configuration();
		Job job = new Job(conf);
		job.setJobName("exemplar");
		job.setJarByClass(findExemplar.class);
		//Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //Setting the mapper and reducer classes
	    job.setMapperClass(ExampleMapper.class);
	    job.setReducerClass(ExampleReducer.class);
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    Path input=new Path(path+"/Similarity");//The input directory path 
	    Path output=new Path(path+"/Evidence");//the output path
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }	
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);	 
	    job.waitForCompletion(true);
	    }
	}
