package org.allocation;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.allocation.utils.DatabaseProvider;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;


public class CARSOutputGenerator {
	
	public static void main (String[] args){
		
		generateRanks();
		
		generateCARSOutput();

}


	private static void generateCARSOutput() {
		//first step, assume input as the number of 
		
		Connection con;
		try {
			
			PrintWriter writer = new PrintWriter(Configuration.CARS_OUTPUT);
			con = DatabaseProvider.connect();
			String query = "select resource, concat(actname,item) as t, group_concat(trank), hour(acttime), casefamiliarity, actfamiliarity, multitask " +
					"from ranalysisj  where resource>0 and month(acttime) in (1,2,3) and trank>=0 and multitask < 15 " +
					" group by resource, t, hour(acttime), casefamiliarity, actfamiliarity, multitask"; 
			//generate ranks for each activity
			
			/*String query = "select resource, concat(actname,item) as t, group_concat(trank) " +
					"from ranalysisj  where resource>0 and month(acttime) in (1,2,3) and trank>=0 and multitask < 25 " +
					" group by resource, concat(actname,item)";*/
			
		/*	String query = "select resource, concat(actname,item) as t, group_concat(trank), hour(acttime) " +
					"from ranalysisj  where resource>0 and resource is not null and month(acttime) in (1,2,3) and trank>=0 and multitask < 15 " +
					" group by resource,t, hour(acttime)";*/
			
			
			
		    Statement selStatement = con.createStatement();
		    ResultSet rsQuery = selStatement.executeQuery(query); 
		    ResultSetMetaData metadata = rsQuery.getMetaData();
		    int columnCount = metadata.getColumnCount();    
		   //first three columns are userid, itemid and rating
		    StringBuilder header = new StringBuilder();
		    header.append("userid,itemid,rating,dc,");
		    for (int i = 4; i <= columnCount; i++) {
		        header.append(metadata.getColumnName(i) + ", ");      
		    }
		   writer.println( header.subSequence(0, header.length()-1));
		   Percentile p = new Percentile();
		   //now get the information and
		   while(rsQuery.next()){
			   StringBuilder row = new StringBuilder();
		       int resourceid = rsQuery.getInt(1);
		       String item = rsQuery.getString(2);
		       String rating = rsQuery.getString(3);
		       System.out.println(rating);
		       //compute the values
		       String[] vals = rating.split(",",1024);
		       int len = vals.length;
		       double[] d = new double[vals.length];
		       for(int j=0;j<vals.length;j++) {
		    	   if(vals[j].length() ==0) continue;
		    	   d[j]=Double.valueOf(vals[j]);
		       }
		    
		       double nrating = p.evaluate(d, 0,len , 0.50);
		   //   nrating=mean.evaluate(d, 0, len);
		       row.append(resourceid).append(",").append(item).append(",").append(nrating).append(",1,");
		       for (int i = 4; i <= columnCount; i++) {
			        row.append(rsQuery.getString(i) + ", ");      
			    }
		       writer.println(row.substring(0,row.length()-1));
		       
		    }
		   
		   rsQuery.close();
		   writer.close();
		   selStatement.close();
		   con.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}






	private static void generateRanks() {
		
		
		Connection con;
		try {
			con = DatabaseProvider.connect();
			String getTurnTime = "select concat(actname, item) as t, avg(turntime), std(turntime)from ranalysisj  where month(acttime) in (11,12,1,2)  group by t";
			//generate ranks for each activity
		    Statement selStatement = con.createStatement();
		    PreparedStatement selRank = con.prepareStatement("update ranalysisj set trank=? where id=?"); 
		    Statement stmtTurnTime = con.createStatement();
		    ResultSet rsTime = stmtTurnTime.executeQuery(getTurnTime); 
		    HashMap<String,Double> turnTimeActivityMap = new HashMap<String,Double>();
		    HashMap<String,Double> stdActivityMap = new HashMap<String,Double>();
		    while(rsTime.next()){
		    	String actName = rsTime.getString(1);
		    	double turnTime = rsTime.getDouble(2);
		    	double stdDev = rsTime.getDouble(3);
		    	turnTimeActivityMap.put(actName, turnTime);
		    	stdActivityMap.put(actName, stdDev);
		    }
		    rsTime.close();
		    ResultSet rsSel = selStatement.executeQuery("select id, concat(actname,item), turntime from ranalysisj where month(acttime) in (1,2,3)");
		       while (rsSel.next()){
		        	int id = rsSel.getInt(1);
		        	String actName = rsSel.getString(2);
		        	double ttime = rsSel.getDouble(3);
		        	//compute the rank
		        	double meanTime = turnTimeActivityMap.get(actName);
		        	double stdev = stdActivityMap.get(actName);
		        	if(stdev==0.0) stdev=1.0;
					double vt = (double)(ttime-meanTime)/(0.15*stdev);
		        //	double vt = (double)(ttime-meanTime)/2;
					double t = (1/(1+Math.exp(vt)));
					int rank = (int)(t*10);
		        	selRank.setInt(1, rank);
		        	selRank.setInt(2, id);
		        	selRank.executeUpdate();
		        }
		       rsSel.close();
		       selStatement.close();
		       selRank.close();
		       stmtTurnTime.close();
		       con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
	
	
}
