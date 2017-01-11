package Cosine_AP;

import java.io.IOException;
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


public class TF_IDF {
	public static class TF_IDFMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			/**
			 * Key is each line in a the file and value is the line as a string
			 * Takes this type of file as an input:
			 * INPUT===>	word@Document n/N
			 * 				word2@Document n/N
			 * 				....
			 * 
			 * OUTPUT====> <word, doc=n/N>
			 */
			String[] Word_and_Doc=value.toString().split("@");
			String[]DocAndCount=Word_and_Doc[1].split("\t");
			Text KEY=new Text(Word_and_Doc[0]);//This is the word
			Text VALUE=new Text(DocAndCount[0]+"="+DocAndCount[1]);//The word and TF weight
			System.out.println("MAP:"+KEY+"--->"+VALUE);
			context.write(KEY, VALUE);
		}
		
		
	}//end class
	
	//This reducer outputs TF weights for each word
	public static class TF_IDFReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text>value,Context context) throws IOException,InterruptedException{
			int numberOfDocs=496;//this stores the # of docs
			//total freq of this word
			int wordCountInCorpus=0;
			Map<String,Double> tempStorage=new HashMap<String,Double>();//store everything
			
			//initial processing of the docs and n/N
			for(Text val:value){
				
				wordCountInCorpus++;//this word appears in these docs
				String[] DocAndTF=val.toString().split("=");//store doc, n/N
				System.out.println(DocAndTF[1]);
				String[]nOverN=DocAndTF[1].split("/");//get numerical value of n/N
				double WordCountOverWordsPerDoc=Double.parseDouble(nOverN[0].trim())/Double.parseDouble(nOverN[1].trim());
				
				tempStorage.put(DocAndTF[0], WordCountOverWordsPerDoc);
				//put the doc and it's value n/N in the Map for later processing.
			}//end for
			
			//Calculate TFIDF
			for (String document: tempStorage.keySet()){
				double TF=tempStorage.get(document);//n/N
				//do IDF
				double PRE_IDF= (double)numberOfDocs / (double) wordCountInCorpus;
				double IDF=Math.log10(PRE_IDF);
				double TFIDF=0;
				//if log(10)=0 consider only the TF and ignore IDF
				if(IDF!=0){TFIDF=TF*IDF;}else{TFIDF=TF;}
				context.write(new Text(key+"@"+document),new Text(String.valueOf(TFIDF)));
				//OUTPUT <word@document, TF_IDF>
			}// end for
		}
	}//end reducer
	
	
	public static void Find_TF_IDF(String path)throws Exception{
		Configuration conf = new Configuration();
        Job job = new Job(conf, "TF_IDF");
        job.setJarByClass(TF_IDF.class);

	    //Outputting key value pairs as a dictionary (rememb python)
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //Setting the mapper and reducer classes
	    job.setMapperClass(TF_IDFMapper.class);
	    job.setReducerClass(TF_IDFReducer.class);
	    
	    //Setting the type of input format. In this case, plain TEXT
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path input=new Path(path+"/unique");//The input directory path 
	    Path output=new Path(path+"/TF_IDF");//the output path
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(output)) {
		      fs.delete(output, true);
		    }
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.waitForCompletion(true);
	}
}
