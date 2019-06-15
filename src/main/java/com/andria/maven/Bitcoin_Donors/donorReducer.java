package com.andria.maven.Bitcoin_Donors;

import com.andria.maven.Bitcoin_Donors.Helper_Methods.ComparatorByBitcoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import com.andria.maven.Bitcoin_Donors.Writable.TransactionJoined;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class donorReducer extends Reducer<NullWritable, Text,NullWritable, Text>{
	
	//We need to rebuild data using the ReadField method of serialized objecgt passed from the mapper to this reducer
	
    Text Output = new Text();
	ArrayList<TransactionJoined>myTransactionRepo = new ArrayList<>();
	
	
	//This value will dictate how many results to be returned for the top nth Donor transactions to wikileaks
	int topWhatever =20;

		
	     
	
	// This reducer will receive partitioning of my dataset based on the amount of bitcoins involved in each transaction
	// This is dictated by the NaturalKeyPrtitioner. Data coming in will be brought to the reducer in the following format:
	//   

	   	@Override
	   	public void reduce( NullWritable key, Iterable<Text>values, Context context) throws IOException, InterruptedException{

	   		
	   		for (Text a : values) {
	   			
	   			String joinedReducer = a.toString();
	   			
	   			String [] split = joinedReducer.split(",");
	   			
	   			TransactionJoined ab = new TransactionJoined();
	   			
	   			//int btc=Integer.parseInt(split[4]);
	   			
	   			ab.inflate(split[0],split[1],split[2],split[3],split[4]);
	   			
	   			
	   			
	   			myTransactionRepo.add(ab);
	   			
	   		
		   			}
		   			
	   		
	   		//Compare by bitcoin value, Sort array
	   		
	   	Collections.sort(myTransactionRepo, new ComparatorByBitcoin());
	   	 
	   	TransactionJoined temp;
	   	 //get the top nth donors based on the value of N being the global variable above 
	   	 
	   	
	   	 for (int i=0; i<topWhatever;i++)
	   	 {
	   		 temp = myTransactionRepo.get(i);
	   		 
	   		
	 		String FinalOutput = String.format("%s,%s,%s,%s","Hash:"+ temp.getHash(),"ID:"+temp.getId(),"Vout:"+temp.getVout(),"N:"+temp.getN());
	 		
	 		String val="BTC: "+temp.getBtc().toString();
	 		
	 		Text FinalOut = new Text(FinalOutput+val);
	 		
	 		//context.write(NullWritable.get(),FinalOut );
	 		
	 		
	 		context.write(NullWritable.get(),FinalOut );
	   		 
	   		 
	   	 }
	   	 
	   	 
	   

	   		
	   	
	   	  
	   	  
	 	
	   		}


}
