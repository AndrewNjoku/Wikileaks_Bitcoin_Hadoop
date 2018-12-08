package Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TransactionJoined  {
	

	//Will contain all fields of transactionout and transaction in joined together
	//This transaction hash was used to join the two data sets and as such i will emit one version 
	
    private Text hash;
	//this is equal to the vout field, if the coins have been respent
	private Text txid;	
	//If these two fields are equal then the money from the transaction has been respecnt. This is highly likely
	private Text n;
	
	private Text vout;
	//Value will be used to sort obviously
	private IntWritable value;
	
	public TransactionJoined(){
		
		hash= new Text();
		txid=new Text();
		n=new Text();
		vout=new Text();
		value=new IntWritable();
	
	}


	public Text getHash() {
		// TODO Auto-generated method stub
		return hash;
	}



	public Text getId() {
		// TODO Auto-generated method stub
		return txid;
	}



	public Text getVout() {
		// TODO Auto-generated method stub
		return vout;
	}



	public Text getN() {
		// TODO Auto-generated method stub
		return n;
	}



	public IntWritable getBtc() {
		// TODO Auto-generated method stub
		return value;
	}



	public void inflate(String shash, String id, String n, String vout, String btc) {
		// TODO Auto-generated method stub
		
		
		hash.set(shash);
		txid.set(id);
		this.n.set(n);
		this.vout.set(vout);
		int vo = Integer.parseInt(btc);
		value.set(vo);

		
	}
     }


