package org.openmucextensions.datalogger.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.openmuc.framework.data.DoubleValue;
import org.openmuc.framework.data.Flag;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.datalogger.spi.LogChannel;
import org.openmuc.framework.datalogger.spi.LogRecordContainer;

/**
 * Wrapper class for H2 Logger database
 * 
 * @author Mike Pichler
 *
 */
public class DatabaseWrapper {
	
	private Connection connection = null;
	
	/**
	 * Connects to the specified H2 database or creates a new database file, if the database doesn't exist.
	 * If the database doesn't contain the necessary tables they will be created.
	 * @param databasename the database name
	 * @throws SQLException if an error occurs while accessing the database
	 * @throws ClassNotFoundException if the H2 JDBC driver couldn't be found
	 */
	public void connect(String databasename) throws SQLException, ClassNotFoundException {
		Class.forName("org.h2.Driver");
		connection = DriverManager.getConnection("jdbc:h2:" + databasename, "sa", "");
		createTables();
	}
	
	/**
	 * Disconnects from the database.
	 * @throws SQLException if any error occurs
	 */
	public synchronized void disconnect() throws SQLException {
		if(connection != null) connection.close();
	}
	
	/**
	 * Adds or updates channels information in the database.
	 * @param channel channel information
	 * @throws SQLException if any error occurs
	 */
	public synchronized void addOrUpdateChannel(LogChannel channel) throws SQLException {
		
		
		PreparedStatement statement = connection.prepareStatement("MERGE INTO CHANNELS(ID, DESCRIPTION, UNIT, LAST_INIT) KEY(ID) VALUES(?, ?, ?, ?);");
		
		statement.setString(1, channel.getId());
		statement.setString(2, channel.getDescription());
		statement.setString(3, channel.getUnit());
		statement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
		statement.execute();
		statement.close();
		
	}
	
	/**
	 * Logs the records in the database.
	 * 
	 * @param containers containers holding the records to log
	 * @param timestamp timestamp for all records
	 * @throws SQLException if any error occurs
	 */
	public synchronized void logValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;

		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO LOGVALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		for (LogRecordContainer logRecordContainer : containers) {
			
			if(logRecordContainer!=null) {
				
				if(logRecordContainer.getRecord()!=null && logRecordContainer.getChannelId()!=null) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					
					if(logRecordContainer.getRecord()!=null) {
						statement.setDouble(3, logRecordContainer.getRecord().getValue().asDouble());
						
						if(logRecordContainer.getRecord().getFlag()!=null) {
							statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
						} else {
							statement.setInt(4, Flag.VALID.getCode());
						}
						
						statement.addBatch();
					}
					
				}		
			}
		}
		
		statement.executeBatch();
		statement.close();
		
	}
	
	/**
	 * Gets the records for the specified channel in the specified time span.
	 * @param channelId the channel id
	 * @param startTime start time
	 * @param endTime end time
	 * @return list of records
	 * @throws SQLException if any error occurs
	 */
	public synchronized List<Record> getRecords(String channelId, long startTime, long endTime) throws SQLException {
		
		PreparedStatement statement = connection.prepareStatement("SELECT * FROM LOGVALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
		statement.setString(1, channelId);
		statement.setTimestamp(2, new Timestamp(startTime));
		statement.setTimestamp(3, new Timestamp(endTime));
		
		ResultSet result = statement.executeQuery();
		
		List<Record> records = new ArrayList<>();
		
		while(result.next()) {
			DoubleValue value = new DoubleValue(result.getDouble("VALUE"));
			long timestamp = result.getTimestamp("TIMESTAMP").getTime();
			Flag flag = Flag.newFlag(result.getInt("FLAG"));
			Record record = new Record(value, timestamp, flag);
			records.add(record);
		}
		
		result.close();
		return records;
	}
	
	/**
	 * Deletes all records that are older than the specified timestamp.
	 * @param timestamp
	 * @throws SQLException
	 */
	public synchronized int deleteRecordsBefore(long timestamp) throws SQLException {
		
		PreparedStatement statement = connection.prepareStatement("DELETE FROM LOGVALUES WHERE TIMESTAMP<?");
		statement.setTimestamp(1, new Timestamp(timestamp));
		int result = statement.executeUpdate();
		statement.close();
		
		return result;
	}
	
	private void createTables() throws SQLException {
		
		Statement statement = connection.createStatement();
		statement.execute("CREATE TABLE IF NOT EXISTS CHANNELS(ID VARCHAR(255) PRIMARY KEY, DESCRIPTION VARCHAR(255), UNIT VARCHAR(255), LAST_INIT TIMESTAMP);");
		statement.execute("CREATE TABLE IF NOT EXISTS LOGVALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE DOUBLE, FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
		statement.close();
		
	}
}
