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

import org.openmuc.framework.data.BooleanValue;
import org.openmuc.framework.data.DoubleValue;
import org.openmuc.framework.data.Flag;
import org.openmuc.framework.data.IntValue;
import org.openmuc.framework.data.LongValue;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.data.StringValue;
import org.openmuc.framework.data.Value;
import org.openmuc.framework.data.ValueType;
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
		
		
		PreparedStatement statement = connection.prepareStatement("MERGE INTO CHANNELS(ID, DESCRIPTION, UNIT, LAST_INIT, VALUE_TYPE) KEY(ID) VALUES(?, ?, ?, ?, ?);");
		
		try {
			statement.setString(1, channel.getId());
			statement.setString(2, channel.getDescription());
			statement.setString(3, channel.getUnit());
			statement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			statement.setString(5, channel.getValueType().name());
			statement.execute();
		} finally {
			if(statement!=null) statement.close();
		}
				
	}
	
	public synchronized void logDoubleValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;
		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO DOUBLE_VALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		try {
			for (LogRecordContainer logRecordContainer : containers) {

				if(isContainerValid(logRecordContainer)) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					statement.setDouble(3, logRecordContainer.getRecord().getValue().asDouble());
					
					if (logRecordContainer.getRecord().getFlag() != null) {
						statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
					} else {
						statement.setInt(4, Flag.VALID.getCode());
					}

					statement.addBatch();
				}
				
			}
			
			statement.executeBatch();
		} finally {
			if(statement!=null) statement.close();
		}
	}
	
	public synchronized void logIntValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;
		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO INT_VALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		try {
			for (LogRecordContainer logRecordContainer : containers) {

				if(isContainerValid(logRecordContainer)) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					statement.setInt(3, logRecordContainer.getRecord().getValue().asInt());
					
					if (logRecordContainer.getRecord().getFlag() != null) {
						statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
					} else {
						statement.setInt(4, Flag.VALID.getCode());
					}

					statement.addBatch();
				}
				
			}
			
			statement.executeBatch();
		} finally {
			if(statement!=null) statement.close();
		}
	}
	
	public synchronized void logLongValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;
		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO LONG_VALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		try {
			for (LogRecordContainer logRecordContainer : containers) {

				if(isContainerValid(logRecordContainer)) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					statement.setLong(3, logRecordContainer.getRecord().getValue().asLong());
					
					if (logRecordContainer.getRecord().getFlag() != null) {
						statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
					} else {
						statement.setInt(4, Flag.VALID.getCode());
					}

					statement.addBatch();
				}
				
			}
			
			statement.executeBatch();
		} finally {
			if(statement!=null) statement.close();
		}
	}
	
	public synchronized void logBoolValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;
		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO BOOL_VALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		try {
			for (LogRecordContainer logRecordContainer : containers) {

				if(isContainerValid(logRecordContainer)) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					statement.setBoolean(3, logRecordContainer.getRecord().getValue().asBoolean());
					
					if (logRecordContainer.getRecord().getFlag() != null) {
						statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
					} else {
						statement.setInt(4, Flag.VALID.getCode());
					}

					statement.addBatch();
				}
				
			}
			
			statement.executeBatch();
		} finally {
			if(statement!=null) statement.close();
		}
	}
	
	public synchronized void logStringValues(List<LogRecordContainer> containers, long timestamp) throws SQLException {
		
		if(containers==null) return;
		
		Timestamp sqlTimestamp = new Timestamp(timestamp);
		
		PreparedStatement statement = connection.prepareStatement("INSERT INTO STRING_VALUES(ID, TIMESTAMP, VALUE, FLAG) VALUES(?, ?, ?, ?);");
		
		try {
			for (LogRecordContainer logRecordContainer : containers) {

				if(isContainerValid(logRecordContainer)) {
					statement.setString(1, logRecordContainer.getChannelId());
					statement.setTimestamp(2, sqlTimestamp);
					statement.setString(3, logRecordContainer.getRecord().getValue().asString());
					
					if (logRecordContainer.getRecord().getFlag() != null) {
						statement.setInt(4, logRecordContainer.getRecord().getFlag().getCode());
					} else {
						statement.setInt(4, Flag.VALID.getCode());
					}

					statement.addBatch();
				}
				
			}
			
			statement.executeBatch();
		} finally {
			if(statement!=null) statement.close();
		}
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
		
		List<Record> records = new ArrayList<>();
		
		String valueTypeString = getChannelValueType(channelId);
		if(valueTypeString==null) {
			// channel not found in database
			return records;
		}
		ValueType valueType = ValueType.valueOf(valueTypeString);
		
		PreparedStatement statement = null;
		
		switch (valueType) {
		case LONG:
			statement = connection.prepareStatement("SELECT * FROM LONG_VALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
			break;
		case INTEGER:
		case SHORT:
		case BYTE:
			statement = connection.prepareStatement("SELECT * FROM INT_VALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
			break;
		case BOOLEAN:
			statement = connection.prepareStatement("SELECT * FROM BOOL_VALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
			break;
		case BYTE_ARRAY:
		case STRING:
			statement = connection.prepareStatement("SELECT * FROM STRING_VALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
			break;
		default:
			statement = connection.prepareStatement("SELECT * FROM DOUBLE_VALUES WHERE ID=? AND TIMESTAMP BETWEEN ? AND ?;");
			break;
		}
		
		statement.setString(1, channelId);
		statement.setTimestamp(2, new Timestamp(startTime));
		statement.setTimestamp(3, new Timestamp(endTime));
		
		ResultSet result = null;
		Value value = null;
		
		try {
			result = statement.executeQuery();
			
			while (result.next()) {
				switch (valueType) {
				case LONG:
					value = new LongValue(result.getLong("VALUE"));
					break;
				case INTEGER:
				case SHORT:
				case BYTE:
					value = new IntValue(result.getInt("VALUE"));
					break;
				case BOOLEAN:
					value = new BooleanValue(result.getBoolean("VALUE"));
					break;
				case BYTE_ARRAY:
				case STRING:
					value = new StringValue(result.getString("VALUE"));
					break;
				default:
					value = new DoubleValue(result.getDouble("VALUE"));
					break;
				}
					
				long timestamp = result.getTimestamp("TIMESTAMP").getTime();
				Flag flag = Flag.newFlag(result.getInt("FLAG"));
				Record record = new Record(value, timestamp, flag);
				records.add(record);
			} 
		} finally {
			if(result!=null) result.close();
			if(statement!=null) statement.close();
		}
		
		return records;
	}
	
	/**
	 * Deletes all records that are older than the specified timestamp.
	 * @param timestamp
	 * @throws SQLException
	 */
	public synchronized int deleteRecordsBefore(long timestamp) throws SQLException {
		
		PreparedStatement statement = null;
		int result = 0;
		
		try {
			statement = connection.prepareStatement("DELETE FROM DOUBLE_VALUES WHERE TIMESTAMP<?");
			statement.setTimestamp(1, new Timestamp(timestamp));
			result = result + statement.executeUpdate();
		} finally {
			if(statement!=null) statement.close();
		}
		
		try {
			statement = connection.prepareStatement("DELETE FROM LONG_VALUES WHERE TIMESTAMP<?");
			statement.setTimestamp(1, new Timestamp(timestamp));
			result = result + statement.executeUpdate();
		} finally {
			if(statement!=null) statement.close();
		}
		
		try {
			statement = connection.prepareStatement("DELETE FROM INT_VALUES WHERE TIMESTAMP<?");
			statement.setTimestamp(1, new Timestamp(timestamp));
			result = result + statement.executeUpdate();
		} finally {
			if(statement!=null) statement.close();
		}
		
		try {
			statement = connection.prepareStatement("DELETE FROM BOOL_VALUES WHERE TIMESTAMP<?");
			statement.setTimestamp(1, new Timestamp(timestamp));
			result = result + statement.executeUpdate();
		} finally {
			if(statement!=null) statement.close();
		}
		
		try {
			statement = connection.prepareStatement("DELETE FROM STRING_VALUES WHERE TIMESTAMP<?");
			statement.setTimestamp(1, new Timestamp(timestamp));
			result = result + statement.executeUpdate();
		} finally {
			if(statement!=null) statement.close();
		}
			
		return result;
	}
	
	private String getChannelValueType(String channelId) throws SQLException {
		
		PreparedStatement statement = connection.prepareStatement("SELECT VALUE_TYPE FROM CHANNELS WHERE ID=?;");
		statement.setString(1, channelId);
		
		try {
			ResultSet result = statement.executeQuery();
			if (result.next()) {
				return result.getString("VALUE_TYPE");
			} else {
				return null;
			} 
		} finally {
			statement.close();
		}
		
	}
	
	private void createTables() throws SQLException {
		
		Statement statement = null;
		
		try {
			statement = connection.createStatement();
			statement.execute("CREATE TABLE IF NOT EXISTS CHANNELS(ID VARCHAR(255) PRIMARY KEY, DESCRIPTION VARCHAR(255), UNIT VARCHAR(255), LAST_INIT TIMESTAMP, VALUE_TYPE VARCHAR(32));");
			statement.execute("CREATE TABLE IF NOT EXISTS DOUBLE_VALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE DOUBLE, FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
			statement.execute("CREATE TABLE IF NOT EXISTS LONG_VALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE BIGINT, FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
			statement.execute("CREATE TABLE IF NOT EXISTS INT_VALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE INT, FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
			statement.execute("CREATE TABLE IF NOT EXISTS BOOL_VALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE BOOLEAN, FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
			statement.execute("CREATE TABLE IF NOT EXISTS STRING_VALUES(ID VARCHAR(255), TIMESTAMP TIMESTAMP, VALUE VARCHAR(1024), FLAG INT, PRIMARY KEY (ID, TIMESTAMP));");
		} finally {
			if(statement!=null) statement.close();
		}
				
	}
	
	/**
	 * Checks the specified container if it's valid (no mandatory properties are null or empty).
	 * 
	 * @param container the container to check
	 * @return true if the container is valid
	 */
	private boolean isContainerValid(LogRecordContainer container) {
		
		if(container==null) return false;
		
		if(container.getChannelId()==null || container.getChannelId().isEmpty()) return false;
		if(container.getRecord()==null) return false;
		if(container.getRecord().getTimestamp()==null) return false;
		if(container.getRecord().getValue()==null) return false;
		
		return true;
	}
}
