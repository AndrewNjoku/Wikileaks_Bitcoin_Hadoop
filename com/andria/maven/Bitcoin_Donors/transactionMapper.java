package com.andria.maven.Bitcoin_Donors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Helper_Methods.IOMethods;
import Writable.TransactionInWriatable;
import Writable.TransactionJoined;
import Writable.TransactionOutWritable;

public class transactionMapper extends Mapper<LongWritable, Text, NullWritable,Text>  {
	
	public static final Log LOG = LogFactory.getLog(WikileaksJoinDriver.class);
	
	//Dictionary which stores a pair containing a string flag in the form of relevant hashes
	// followed by a POJO object stroing relevant fields in a contextualised fashion.
	
	Map <String, TransactionOutWritable> Dictionary = new HashMap<>();
	
	//Output from mapper logic will set these fields to be passed to context 
	
	//public NullWritable outputKey = new NullWritable(null);
	
	private Text outputValue = new Text();

	@Override
	public void setup(Context context) throws IOException, InterruptedException, NullPointerException {

		boolean cacheOK = false;

		URI [] cacheFiles = context.getCacheFiles();
		
		//we can do a check to make sure that the correct 
		
		final String CacheFilename = context.getConfiguration().get(WikileaksJoinDriver.TOUT_FILENAME_CONF_KEY);


		
		if (cacheFiles != null && cacheFiles.length > 0) 
		  {
			
			Path cachefilepath = new Path(cacheFiles[0].toString());
	
		    FileSystem fs = FileSystem.get(context.getConfiguration());
            
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(cachefilepath)));

            //Read the file please!!
            
            String TransactionLine;
            

            while((TransactionLine = bufferedReader.readLine()) != null) {
           	 
           	 
           //if line fits expected format create a transaction object for this line
            	
            	
           TransactionOutWritable myTransactionOut = new TransactionOutWritable();
           
           
           System.err.println("This is the line being passed to transaction to parse" + TransactionLine);
           	
             
             myTransactionOut.parseLine(TransactionLine);
             
             //key is the hash 
            
             String key = myTransactionOut.getKey();
             
             //Temporary solution!!! take this out once fixed
             
              key = key.startsWith("\"") ? key.substring(1) : key;
        
             
             //putting tout hash as key together with the tout object holding conbtextualised info
             
             
             Dictionary.put(key, myTransactionOut);
			
			
			cacheOK = true;
			
		    }
            
            
        	System.err.println(" This is the TransactionOutput Map we are dealing with" + Dictionary.toString());
		  }
		
		
		// if we get to this point without hte cache ok flag being triggered it means that we havent 
			//got a chache file or the names dont add up and hasnt been found 
		
		if (!cacheOK) {
			System.err.println("Distributed cache file not found : " + CacheFilename);
			
			throw new IOException("Distributed cache file not found : " + CacheFilename);
		}
       }
	
	
	

	

	@Override
	public void map(LongWritable key, Text tInLine, Context context)
			throws IOException, InterruptedException {
		String lineIn = tInLine.toString();

		//parse the text file containing input data set
		//into newly created Tin object
		TransactionInWriatable tin = new TransactionInWriatable();

		tin.parseLine(lineIn);
	
		TransactionOutWritable tout = Dictionary.get(tin.getHashForJoin());
		
		// Ignore if the corresponding entry doesn't exist in the projects data (INNER JOIN)
		if (tout == null) {
			

			System.out.println(" This Map iteration is null , no join performed ");
			
			return;
			
		}
		
		
		
		
		
		String combined = String.format("%s,%s,%s,%s,%s" ,tin.tx_hash, tin.txid, tin.vout_magnet,tout.n,tout.value);
        
		
		System.out.println("Testing Combined: " + combined);
	
		
		Text myOutput = new Text();
				
				myOutput.set(combined);
		
		
		
		
		//we have now got two matching transaction objects, we will join the two into a single object and pass as value together with a key 
		//containing the hash value of the transaction which bound the two in the first place
		
		/*
		
		String TinOutput = String.format("%s,%s,%s|", tin.tx_hash, tin.txid, tin.vout_magnet);
		String ToutOutput = String.format("|%s,%s,%s", tout.hash, tout.n,tout.value);
        
        */
		
		
		
		

		//keys is nullwritable as it dont matter, we are doing the sorting in the reducer 
		
		context.write(NullWritable.get() , myOutput);
	}


}
