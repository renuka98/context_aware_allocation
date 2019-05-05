package org.allocation;

import carskit.main.CARSKit;


public class CARSActivator {
	
	public static void main(String[] args) {		
	    try {
	    	
	    	args =new String[] {"-c", Configuration.CARS_CONF_LOCATION};
            CARSKit.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
