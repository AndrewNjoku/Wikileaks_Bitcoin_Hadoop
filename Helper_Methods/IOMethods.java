package Helper_Methods;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Writable.TransactionOutWritable;

public class IOMethods {
	


public static Map<String, TransactionOutWritable> createTransactionFromFile (Path filePath, Context context) throws IOException, FileNotFoundException  {
	
	Map<String,TransactionOutWritable> myDictionary = new HashMap<>();
	 
	
	
	 System.err.println("in IO method creating transaction from file ");
	 
	                 FileSystem fs = FileSystem.get(context.getConfiguration());
	                 
		             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		 
		             String TransactionLine;
		 
		             while((TransactionLine = bufferedReader.readLine()) != null) {
		            	 
		            	 
		            TransactionOutWritable myTransactionOut = new TransactionOutWritable();
		            
		            
		            System.err.println("This is the line being passed to transaction to parse" + TransactionLine);
		            	
		              
		              myTransactionOut.parseLine(TransactionLine);
		              
		              //key is the hash 
		             
		              String key = myTransactionOut.getKey();
		              
		              
		              //Each transaction hash is now a key in a map to a POJO holding information about that 
		              //particular transaction 
		           //   myDictionary = new HashMap<>();
		              
		              myDictionary.put(key, myTransactionOut);
		              
		              
		             }
		           
		 
		         
	
	return myDictionary;
	
		 
		     }


}

