package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TransactionJoined implements Writable, WritableComparable<TransactionJoined>{
	

	//Will contain all fields of transactionout and transaction in joined together
	//This transaction hash was used to join the two data sets and as such i will emit one version 
	
    public Text hash = new Text();
	
	//this is equal to the vout field, if the coins have been respent
	public Text txid = new Text();

	
	//If these two fields are equal then the money from the transaction has been respecnt. This is highly likely
	public Text n = new Text();
	
	public Text vout = new Text();
	
	//Value will be used to sort obviously
	public IntWritable value = new IntWritable();

	
	
	public void Join( TransactionOutWritable out, TransactionInWriatable in)
	{
		
		hash.set(out.hash);
		txid.set(in.txid);
		n.set(out.n);
		vout.set(in.vout_magnet);
		value=out.value;
		
		
		
	}

	@Override
	public int compareTo(TransactionJoined o) {
		// TODO Auto-generated method stub
		
		int res=0;
	       
		res = this.value.compareTo(o.value);
		
	    return res;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public String toString() {
		
		// we dont need hash as its going to be in the first column 
		
		return " ID:" +txid+", N:"+n+", Vout:"+vout+ ", Bitcoin:" + value;
		
		
		
	}

}
