package org.allocation;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.allocation.utils.DatabaseProvider;

import java.util.Map.Entry;


public class CARSDataPreprocessor {

	public static void main(String[] args) {
		
 //updatePinstance();

		generateResourceData();		
		
	}

	

	private static void updatePinstance() {
		try {	
			 Connection con = DatabaseProvider.connect();
			 //PreparedStatement inStmt = con.prepareStatement("insert into pinstance1 (caseid,activityname,resource,starttime,amountreq,registration) values(?,?,?,str_to_date(?,'%Y-%m-%dT%T.%f'),?,str_to_date(?,'%Y-%m-%dT%T.%f'))");
			 PreparedStatement inStmt = con.prepareStatement("insert into pinstance1 (caseid,activityname,resource,starttime,amountreq,registration) values(?,?,?,?,?,?)");
			 PreparedStatement upStmt = con.prepareStatement("update pinstance1 set endtime=? where resource=? and caseid=? and activityname=? and ?>starttime and endtime is null;");
			 PreparedStatement selStmt = con.prepareStatement("select caseid,activityname,status,acttime,resource,amountreq,registration from pinstance where caseid=? and activityname like 'W_%' and status in ('START','COMPLETE') and not resource  in (112,-1) order by acttime");
	        Statement selStatement = con.createStatement();
	        ResultSet rsSel = selStatement.executeQuery("select distinct caseid from pinstance");
	        while (rsSel.next()){
	        	int caseid = rsSel.getInt(1);
	        	selStmt.setInt(1, caseid);
	        	ResultSet rsCase = selStmt.executeQuery();
	        	
	        	while (rsCase.next()){
	        		//get each activity name, case id, 
	        		int cId = rsCase.getInt(1);
	        		String actName = rsCase.getString(2);
	        		String status = rsCase.getString(3);
	        		Timestamp actTime = rsCase.getTimestamp(4);
	        		int res = rsCase.getInt(5);
	        		int amount = rsCase.getInt(6);
	        		Timestamp registration = rsCase.getTimestamp(7);
	        		
	        		if(status.equals("START")) {
	        			inStmt.setInt(1, cId);
	        			inStmt.setString(2, actName);
	        			inStmt.setInt(3, res);
	        			inStmt.setTimestamp(4, actTime);
	        			inStmt.setInt(5, amount);
	        			inStmt.setTimestamp(6, registration);
	        			inStmt.executeUpdate();
	        		}
	        		else if (status.equals("COMPLETE")){
	        			upStmt.setTimestamp(1, actTime);
	        			upStmt.setInt(2, res);
	        			upStmt.setInt(3, cId);
	        			upStmt.setString(4, actName);
	        			upStmt.setTimestamp(5, actTime);
	        			upStmt.executeUpdate();
	        		}
	        	}
	        	rsCase.close();
	        }
	        rsSel.close();
	        
	        inStmt.close();
	         upStmt.close();
	        selStmt.close();
	        selStatement.close();
	        
	         
	         con.close();
	      } catch (Exception e) {
	         e.printStackTrace();
	      }
	}
	
	//update ranalysis set item=least(floor(amount/10000),7);
	private static void generateResourceData() {
		try {	
			 Connection con = DatabaseProvider.connect();
			
			 PreparedStatement inStmt = con.prepareStatement("insert into ranalysis (caseid,actname,resource,acttime,amount,turntime) values(?,?,?,?,?,?)");
			 PreparedStatement upStmt = con.prepareStatement("update ranalysis set multitask=? where resource=? and caseid=? and actname=? and acttime=str_to_date(?,'%Y-%m-%d %T.%f')");
			 PreparedStatement selStmt = con.prepareStatement("select caseid,activityname,starttime,endtime,amountreq,datediff(starttime,endtime), timestampdiff(SECOND,starttime,endtime)/60 as t from pinstance1 where resource=? and month(starttime)=? and day(starttime)=? having t>=1 order by starttime");
		//	 PreparedStatement selMulti = con.prepareStatement("select caseid,activityname,starttime from pinstance1 where ((starttime between timestampadd(SECOND,2,?) and ?) or (starttime<? and endtime between ? and timestampdiff(SECOND,2,?))) and resource=?");
		
			 PreparedStatement selMulti=con.prepareStatement("select caseid,activityname,starttime from pinstance1 where ((starttime between ? and ?) or (starttime<? and endtime between ? and ?) or (starttime<? and endtime>?)) and resource=?");
			 PreparedStatement selCaseFam = con.prepareStatement("select count(*) from pinstance1 where resource=? and caseid=? and starttime<?");
			 PreparedStatement selActFam = con.prepareStatement("select count(*) from pinstance1 where resource=? and activityname=? and starttime<? and date(starttime)=date(?)");
			 PreparedStatement selExp = con.prepareStatement("select count(distinct caseid) from pinstance1 where starttime<? and resource=? and date(starttime)=date(?)");
			 PreparedStatement updateAct = con.prepareStatement("update ranalysis set actfamiliarity=? ,casefamiliarity=?, pref=? where resource=? and caseid=? and actname=? and acttime=?");
	        Statement selStatement = con.createStatement();
	        ResultSet rsSel = selStatement.executeQuery("select resource, count(distinct caseid) as t from pinstance group by resource having t >250");
	       while (rsSel.next()){
	       // if(rsSel.next()){
	        	int resId = rsSel.getInt(1);
	        	if(resId ==-1) continue;
	        	//for all days of the month compute the turn around time.
	        	for(int i=1;i<=31;i++){
	        		 HashMap<String,Integer> multiTaskString = new HashMap<String,Integer>();
	        		selStmt.setInt(1, resId);
	        		selStmt.setInt(2,3);  // this is the month setting
	        		selStmt.setInt(3, i);
	        		ResultSet rsDay = selStmt.executeQuery();
	        	
	        	while (rsDay.next()){
	        		//get each activity name, case id, 
	        		int cId = rsDay.getInt(1);
	        		String actName = rsDay.getString(2);
	        		Timestamp stTime = rsDay.getTimestamp(3);
	        		Timestamp edTime = rsDay.getTimestamp(4);
	        		int amount = rsDay.getInt(5);
	        		int datediff = rsDay.getInt(6);
	        		double tat = rsDay.getDouble(7);
	        		//update the information of all the stuff in ranalysis
	        		inStmt.setInt(1, cId);
	        		inStmt.setString(2, actName);
	        		inStmt.setInt(3, resId);
	        		inStmt.setTimestamp(4, stTime);
	        		inStmt.setInt(5, amount);
	        		if(datediff == 0)
	        			inStmt.setDouble(6, tat);
	        		else
	        			inStmt.setDouble(6, -1.0); //manually update these else try ignore them if very few.
	        		
	        		inStmt.executeUpdate();
	        		
	        		//now find the multi-tasking part and update this work
	        		selMulti.setTimestamp(1, stTime);
	        		selMulti.setTimestamp(2, edTime);
	        		selMulti.setTimestamp(3,stTime);
	        		selMulti.setTimestamp(4, stTime);
	        		selMulti.setTimestamp(5, edTime);
	        		selMulti.setTimestamp(6, stTime);
	        		selMulti.setTimestamp(7, edTime);
	        		
	        		selMulti.setInt(8, resId);
	        		
	        		ResultSet rsMulti = selMulti.executeQuery();
	        		boolean hasMultipleTasks = false;
	        		int multiStr = 0;
	        		while(rsMulti.next()){
	        			hasMultipleTasks = true;
	        			multiStr++;
	        			
	        		}
	        		rsMulti.close();
	        		String parentKey = cId + ";"+ actName + ";" + stTime;
	        		multiTaskString.put(parentKey, multiStr);	        
	        		addCaseAndActivityFamiliarity(resId,cId,actName,stTime,selActFam,selCaseFam,updateAct, amount, selExp);
	        	}
	        	
	        	rsDay.close();
	        	//now update the multi-task values given that you have all the information for the day
	        	Set<Entry<String,Integer>> multEntrySet = multiTaskString.entrySet();
	        	for(Entry<String,Integer> eachMulti : multEntrySet) {
	        		String k = eachMulti.getKey();
	        		String[] vals = k.split(";");
	        		int caseid = Integer.valueOf(vals[0]);
	        		String aName = vals[1];
	        		String startTime = vals[2];
	        		int mVal = eachMulti.getValue();
	        		upStmt.setInt(1, mVal-1);
	        		upStmt.setInt(2, resId);
	        		upStmt.setInt(3, caseid);
	        		upStmt.setString(4, aName);
	        		upStmt.setString(5, startTime);
	        		upStmt.executeUpdate();
	        	
	        	}
	        	
	        	}
	        }
	        rsSel.close();
	              
	        selStmt.close();
	        selStatement.close();
	        selMulti.close();
	         inStmt.close();
	         upStmt.close();
	         selActFam.close();
	         selCaseFam.close();
	         updateAct.close();
	         con.close();
	      } catch (Exception e) {
	         e.printStackTrace();
	      }
	}

	private static void addCaseAndActivityFamiliarity(int resId, int cId,
			String actName, Timestamp stTime, PreparedStatement selActFam,
			PreparedStatement selCaseFam, PreparedStatement updateAct, int amount, PreparedStatement selExp) {

		//select count(*) from pinstance1 where resource=? and caseid=? and activityname=? and starttime<?
		try {
			selCaseFam.setInt(1, resId);
			selCaseFam.setInt(2, cId);
			selCaseFam.setTimestamp(3, stTime);
			ResultSet rsCase = selCaseFam.executeQuery();
			int cCount = 0;
			if(rsCase.next())
				cCount = rsCase.getInt(1);
			rsCase.close();
			
			int actCount=0;
			selActFam.setInt(1,resId);
			selActFam.setString(2, actName);
			selActFam.setTimestamp(3, stTime);
			selActFam.setTimestamp(4, stTime);
			ResultSet rsAct = selActFam.executeQuery();
			if(rsAct.next())
				actCount = rsAct.getInt(1);
			rsAct.close();
			//"select count(*) from pinstance1 where resource=? and amount>=round(?/1000) and activityname=? and starttime<?"
			
			selExp.setTimestamp(1, stTime);
			selExp.setInt(2, resId);
			selExp.setTimestamp(3, stTime);
			
			
			ResultSet rsExp = selExp.executeQuery();
			int exp = 0;
			if(rsExp.next())
				exp=rsExp.getInt(1);
			rsExp.close();
			//update ranalysis set activityfamiliarity=? ,casefamiliarity=? where resource=? and caseid=? and actname=? and acttime=?
			updateAct.setInt(1, actCount);
			updateAct.setInt(2, cCount);
			updateAct.setInt(3,  exp);
			updateAct.setInt(4, resId);
			updateAct.setInt(5,cId);
			updateAct.setString(6, actName);
			updateAct.setTimestamp(7, stTime);
			updateAct.executeUpdate();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

		
}
