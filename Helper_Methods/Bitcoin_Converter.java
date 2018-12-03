package Helper_Methods;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.andria.maven.Bitcoin_Donors.WikileaksJoinDriver;

public class Bitcoin_Converter {
	
	
	public static final Log LOG = LogFactory.getLog(WikileaksJoinDriver.class);
	//price for one bitcoin
	private static Double bitcoinPrice = 2000.00;

	
	public static Double convertBitcoin(String string) {
		// TODO Auto-generated method stub
		//example 0.65
		
		double BitcoinDouble;
		
			 
		BitcoinDouble = (double)Double.valueOf(string);
		
		
		
		
		double priceInPounds = BitcoinDouble*bitcoinPrice ;
	
		return priceInPounds;
		
	}
	
	public static boolean isDouble(String s) {
		
	    if(s.isEmpty()) return false;
	    
	    for(int i = 0; i < s.length(); i++) {
	        if(s.charAt(i) == '.') {
	   
	        return true;
	    }
	       
		
	}
	    return false;
	 
	}
}
