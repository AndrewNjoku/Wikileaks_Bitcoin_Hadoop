package com.andria.maven.Bitcoin_Donors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Writable.TransactionJoined;
import Writable.TransactionOutWritable;

public class donorReducer extends Reducer<NullWritable, TransactionJoined, String , String>{
	
	String OutputColumnOne;
	String OutputColumnTwo;
	ArrayList<TransactionJoined>myTransactionRepo = new ArrayList<>();
	
	
	//This value will dictate how many results to be returned for the top nth Donor transactions to wikileaks
	int topWhatever =10;

		
	     
	
	// This reducer will receive partitioning of my dataset based on the amount of bitcoins involved in each transaction
	// This is dictated by the NaturalKeyPrtitioner. Data coming in will be brought to the reducer in the following format:
	//   

	   	@Override
	   	public void reduce( NullWritable key, Iterable<TransactionJoined>values, Context context) throws IOException, InterruptedException{

	   		
	   		for (TransactionJoined a : values) {
	   			
	   			myTransactionRepo.add(a);
	  
		   			}

	   		//using good old fashioned bubble sort 
	   		//can then simply emit the first N values for output
	   		TransactionJoined temp;
	   	
	   	  if (myTransactionRepo.size()>1) // check if the number of orders is larger than 1
	        {
	            for (int x=0; x<myTransactionRepo.size(); x++) // bubble sort outer loop
	            {
	                for (int i=0; i < myTransactionRepo.size()-x; i++) {
	                	
	                	//I can use my custom implementation here 
	                	
	                    if (myTransactionRepo.get(i).compareTo(myTransactionRepo.get(i+1)) > 0)
	                    {
	                        temp = myTransactionRepo.get(i);
	                        myTransactionRepo.set(i,myTransactionRepo.get(i+1) );
	                        myTransactionRepo.set(i+1, temp);
	                    }
	                }
	            }
	        }

	   	  //output the ordered transactions 
	   	
	   	  for(int i=0;i<topWhatever;i++)
	   	  {
	   		 TransactionJoined x = myTransactionRepo.get(i);
	   		  
	   		  
	   		  //First column will just have hashcode
	   		  OutputColumnOne=x.hash.toString();
	   		  
	   		  
	   		  //i have overwritten toString in the object class to properly specify my intended format
	   		  OutputColumnTwo = x.toString();
 
	   			context.write(OutputColumnOne, OutputColumnTwo);
	   			
	   	  }
	 	
	   		}


}
