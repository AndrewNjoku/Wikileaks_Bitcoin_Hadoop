package Helper_Methods;

import java.util.Comparator;

import Writable.TransactionJoined;

public class ComparatorByBitcoin implements Comparator<TransactionJoined> {
	
	

	@Override
	public int compare(TransactionJoined o1, TransactionJoined o2) {
		
		
		// Intwritable has a built in compareTo method so we can use this one without having to overide 
		// in pojo decleration
		int res=0;
	       
		res = o2.getBtc().compareTo(o1.getBtc());
		
	    return res;
	
	}

}
