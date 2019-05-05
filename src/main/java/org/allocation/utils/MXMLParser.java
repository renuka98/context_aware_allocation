package org.allocation.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.allocation.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MXMLParser {

	
	

	public static void main(String[] agrs){
		//read the xml file and load it into a db
		//read process instance and get reg_date and amount_req
		//get all its children and save into DB.
		parseProcessInstanceData();
		
	}

	private static void parseProcessInstanceData() {
		 try {	
			 Connection con = DatabaseProvider.connect();
			 PreparedStatement stmt = con.prepareStatement("insert into pinstance (caseid,activityname,status,acttime,resource,amountreq,registration) values(?,?,?,str_to_date(?,'%Y-%m-%dT%T.%f'),?,?,str_to_date(?,'%Y-%m-%dT%T.%f'))");
	         File inputFile = new File(Configuration.MXML_FILE);
	         DocumentBuilderFactory dbFactory  = DocumentBuilderFactory.newInstance();
	         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	         Document doc = dBuilder.parse(inputFile);
	         doc.getDocumentElement().normalize();
	         System.out.println("Root element :" 
	            + doc.getDocumentElement().getNodeName());
	         NodeList nList = doc.getElementsByTagName("ProcessInstance");
	         System.out.println("----------------------------");
	         for (int temp = 0; temp < nList.getLength(); temp++) {
	            Node nNode = nList.item(temp);
	            System.out.println("\nCurrent Element :" + temp);
	        //    if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	            Element eElement = (Element) nNode;
	            String pid = eElement.getAttribute("id");
	            System.out.println("instance id : "  + pid);
	            Element dNode = (Element)eElement.getElementsByTagName("Data").item(0);
	            String regDate = null;
	            String amountReq = null;
	            NodeList dList = dNode.getElementsByTagName("attribute");
	            for(int j=0;j<dList.getLength();j++) {
	            	Element att = (Element)dList.item(j);
	            	String attributeName = att.getAttribute("name");
	            	String attValue = att.getTextContent();
	            	if(attributeName.equals("REG_DATE"))
	            		regDate=attValue;
	            	else if(attributeName.equals("AMOUNT_REQ"))
	            		amountReq=attValue;
	            }
	             System.out.println(" amount requested = " + amountReq + " registrationDate = " + regDate);
	             int pos = regDate.indexOf('+');
	             regDate = regDate.substring(0,pos);
	             //now get element for audit trail
	             NodeList eventList = eElement.getChildNodes();
	             System.out.println(eventList.getLength());
	             for(int k=0; k < eventList.getLength();k++) {
	            	 Node auditEntry = eventList.item(k);
	            	 if(auditEntry.getNodeName().equals("AuditTrailEntry")){ 
	            	 Element t = (Element)auditEntry;
	            	 Element wnode = (Element)t.getElementsByTagName("Data").item(0);
	 	            String resource = null;
	 	            String wfElement = null;
	 	            String status = null;
	 	            String timeStamp = null;
	 	            NodeList wList = wnode.getElementsByTagName("attribute");
	 	            for(int x=0;x<wList.getLength();x++) {
	 	            	Element att = (Element)wList.item(x);
	 	            	String attributeName = att.getAttribute("name");
	 	            	String attValue = att.getTextContent();
	 	            	if(attributeName.equals("org:resource"))
	 	            		resource=attValue;
	 	            	else if(attributeName.equals("time:timestamp"))
	 	            		timeStamp=attValue;
	 	            	else if(attributeName.equals("concept:name"))
	 	            		wfElement=attValue;
	 	            	else if(attributeName.equals("lifecycle:transition"))
	 	            		status=attValue;
	 	            }
	 	            int npos = timeStamp.indexOf('+');
	 	            timeStamp = timeStamp.substring(0, npos);
	            	 System.out.println(wfElement + " ;" + status + " ;" + resource + " ;" + timeStamp);
	            	 stmt.setInt(1, Integer.valueOf(pid));
	            	 stmt.setString(2, wfElement);
	            	 stmt.setString(3,status);
	            	 stmt.setString(4,timeStamp);
	            	 if(resource == null)
	            		 resource = "-1";
	            	 stmt.setInt(5, Integer.valueOf(resource));
	            	 stmt.setInt(6,Integer.valueOf(amountReq));
	            	 stmt.setString(7,regDate);
	            	 stmt.executeUpdate();
	            	 }
	             }
	              
	   
	            }
	       //  }
	         stmt.close();
	         con.close();
	      } catch (Exception e) {
	         e.printStackTrace();
	      }
		
	}
}
