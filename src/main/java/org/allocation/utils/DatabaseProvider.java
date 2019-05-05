package org.allocation.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.allocation.Configuration;

public class DatabaseProvider {

	private static Connection connection =null;
	
	
	public DatabaseProvider() {
		super();
	}

	

	  public static Connection connect() throws SQLException {
		    if (connection == null) {
		      try {
		        Class.forName(Configuration.jdbcDriverClass);
		      } catch (Exception instantiationException) {
		        throw new SQLException("Unable to load jdbc driver " + 
		                               Configuration.jdbcDriverClass + ":" +
		                               instantiationException.getClass().getName() +
		                               ":" + instantiationException.getMessage());
		      }
		     
		      connection = DriverManager.getConnection(Configuration.databaseURL,
		                                               Configuration.username,
		                                               Configuration.password);
		      connection.setAutoCommit(true);
		      return connection;
		    }
		    else
		    	return connection;
		}
	  
	  
	 
}