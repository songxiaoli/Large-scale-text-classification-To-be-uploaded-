package AP;

import AP.findCluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

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


public class findCluster{
    public static class ClusterMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			
			/*
			 * Input <doc1 and doc2>, <sim, a, r>{
			 * Output<doc1>, <doc2, sim, a, r, true/false> 
			 */
			String[]line = value.toString().split("\t");
			String points = line[0].toString();
			String exemplar = line[1].toString().split(" ")[0];
			String sim = line[1].toString().split(" ")[1];
			context.write(new Text(points), new Text(exemplar+": "+sim));
		}			
	}

    public static class ClusterReducer extends Reducer<Text, Text, Text, Text>{
	
        public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
            
            HashMap<String, Double> simMapping = new HashMap<String, Double>();
            int doc = Integer.parseInt(key.toString()); //this is k not i.
		
            //put all the document data into the data structure
		
            for(Text val:values){
                String[]data = val.toString().split(": ");
                String exemplar = data[0];
                String sim = data[1];
                simMapping.put(	exemplar, Double.parseDouble(sim));
            }
            Iterator<String> iter = simMapping.keySet().iterator();
            boolean first = true;
            double minValue = 0.0;
            String minNo = "0";
            while(iter.hasNext()){
                String exemplar = iter.next().toString();
                double value = simMapping.get(exemplar);
                if(first){
                    minNo = exemplar;
                    minValue = value;
                    first = false;
                }
                if( minValue < value ){
                    minNo = exemplar;
                    minValue = value;
                }
            }
            context.write(key, new Text(minNo));
        }
	}//end method				

	public static void cluster_finder (String path) throws Exception{		
		//do n iterations of responsibility and availibility
		Configuration conf= new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(findCluster.class);
        //job.setJobName("Availibitily and Responsibility");
		
		//Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(ClusterMapper.class);
	    job.setReducerClass(ClusterReducer.class);
	    
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/Evidence");//The input directory path 
	    Path output=new Path(path+"/Cluster");//the output path
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
	      fs.delete(output, true);
	    }	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);	 
	    job.waitForCompletion(true);
	}
}