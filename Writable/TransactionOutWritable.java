package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import Helper_Methods.Bitcoin_Converter;

public class TransactionOutWritable implements Writable {
	
	
	//this price is for one bitcoin
	
	public static Double bitcoinPrice = 2500.00;
	
	//This is a Writable POJO in order to store transaction output information in a serializable format
  
	
	//This is the info that will be cached 
	//we only need to include 
	

     public Text hash = new Text();
	
	//we want to have a comparator target this value 
	 public IntWritable value = new IntWritable();
	
	//This field will be used to make the join between the processed dataset (Vout) and 
	//the much smaller cached dataset we will be holding in memory
	
	//this is equal to the vout field, if the coins have been respent
	
	public Text n = new Text();
	

	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
/*
		hash= arg0.readUTF();
		value= arg0.readUTF();
		n=arg0.readUTF();
		
		*/
		
		
	}
	
	public IntWritable getValue() {
		
		
		return value;
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	public void parseLine(String line) throws IOException {

		
		// Split on commas, unless they were escaped with a backslash 
		// e.g. the "message" field can contain commas.
		// Negative limit param "-1" to keep empty values in resulting array
		String [] parts = line.split(",");
		
		
		
		System.err.println("The size of my split words array is:" + parts.length);
		
		if( parts.length > 0) {
			
			System.err.println("the word array is not empty");
			
			System.err.println(" Putting hash into Transactionout, the line is:");
			
			for (int i=0;i<parts.length;i++) {
				
				System.err.println("word" + i + parts[i]);
				
			}
		
		hash.set(parts[1]);
		
		// we need to convert bitcoin to pounds and store as an integer 
		
		 double bitcoinConverted =Bitcoin_Converter.convertBitcoin(parts[2]);
		 
		 int temp=(int)bitcoinConverted;
				 
		 value.set(temp);
		 
		n.set(parts[3]);
		}
		else { 
			
			System.err.println("the word array is empty");
			
			
		}
	}

	

	public String getKey() {
		// TODO Auto-generated method stub
		return hash.toString();
	}


}
