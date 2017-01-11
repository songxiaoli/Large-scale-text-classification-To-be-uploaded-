package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class wordCountWritable implements Writable{

	private Text word;
	private IntWritable count;
    
	//null arguments
	public wordCountWritable(){
		setWord("");
		setCount(0);
	}
    
	public wordCountWritable(String word,String count){
		setWord(word);
		setCount(Integer.parseInt(count));

	}
	
	public wordCountWritable(String word, int v1) {
		setWord(word);
		setCount(v1);
	}
    
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		word.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		word.write(out);
		count.write(out);
	}

	public int getCount() {
		return count.get();
	}
    
	public void setCount(int count) {
		this.count = new IntWritable(count);
	}
    
	public String getWord() {
		return word.toString();
	}
    
	public void setWord(String word) {
		this.word = new Text(word);
	}

}


