package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TransactionInWriatable implements Writable{
	
	//This is a Writable POJO in order to store transaction output information in a serializable format
	  

	/**
	txid: The associated transaction the coins are going into


   // this tx_hash will be used to join the t_in and t_out sets together 
    
	tx_hash: The transaction the coins are coming from

	vout: The ID of the output from the previous transaction - the value equals n in vout below

	Sample entry:

	    f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16,0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9,0
	
	@author lolo
	**/
	
	
	public Text txid = new Text();
	
	public Text tx_hash = new Text();
	
	
	public Text vout_magnet = new Text();

	
	
	public String getHashForJoin(){
		
		
		String outputHashString = txid.toString();
		
		
		return outputHashString;
	
	}
	
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	
	}
	
	public void parseLine(String line) throws IOException {
		
	
		
		// Split on commas, unless they were escaped with a backslash 
		// e.g. the "message" field can contain commas.
		// Negative limit param "-1" to keep empty values in resulting array
		String[] parts = line.split(",");
		
		txid.set(parts[0]);
		
		tx_hash.set(parts[1]);
		
		vout_magnet.set(parts[2]);
		
	}

	

}
