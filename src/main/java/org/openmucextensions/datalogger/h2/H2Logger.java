package org.openmucextensions.datalogger.h2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.openmuc.framework.data.Record;
import org.openmuc.framework.datalogger.spi.DataLoggerService;
import org.openmuc.framework.datalogger.spi.LogChannel;
import org.openmuc.framework.datalogger.spi.LogRecordContainer;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Logger implements DataLoggerService {

	private static Logger logger = LoggerFactory.getLogger(H2Logger.class);
	
	private DatabaseWrapper database = null;
	private boolean initSuccessful = false;
	private Timer cleanupTimer = null;
	
	private String databaseFilename = null;
	private long storageInterval;
	
	private Map<String, LogChannel> channelsToLog = new ConcurrentHashMap<String, LogChannel>();
	
	protected void activate(ComponentContext context) {
		
		logger.info("Activating H2 logger");
		
		databaseFilename = System.getProperty("org.openmucextensions.datalogger.h2.database", "./h2logger/database");
		
		String storageIntervalString = System.getProperty("org.openmucextensions.datalogger.h2.storageInterval");
		if(storageIntervalString!=null) storageInterval = Long.parseLong(storageIntervalString);
		else storageInterval = 1000*60*60*24*14;
		
		try {
			database = new DatabaseWrapper();
			database.connect(databaseFilename);
			initSuccessful = true;
			
			if(storageInterval!=0) {
				cleanupTimer = new Timer("H2Logger Cleanup Timer", true);
				cleanupTimer.scheduleAtFixedRate(new CleanupTask(database, storageInterval), 10*1000, 1000*60*60*24);
			}
					
		} catch (ClassNotFoundException e) {
			logger.error("H2 database driver not found");
		} catch (SQLException e) {
			logger.error("Error while connecting to database: {}", e.getMessage());
		}
	}
	
	protected void deactivate(ComponentContext context) {
		logger.info("Deactivating H2 logger");
		
		if(cleanupTimer!=null) cleanupTimer.cancel();
		
		if(database != null)
			try {
				database.disconnect();
				logger.debug("Database connection closed");
			} catch (SQLException e) {
				logger.warn("Error while closing database connection: {}", e.getMessage());
			}
	}
	
	@Override
	public String getId() {
		return "h2logger";
	}

	@Override
	public void setChannelsToLog(List<LogChannel> channels) {
		// will be called when OpenMUC starts the logger
		
		if(!initSuccessful) {
			logger.warn("Component initialization wasn't successful, not logging any data");
			return;
		}
		
		channelsToLog.clear();
		for (LogChannel logChannel : channels) {
			channelsToLog.put(logChannel.getId(), logChannel);
			try {
				database.addOrUpdateChannel(logChannel);
			} catch (SQLException e) {
				logger.error("Error while writing log channel information to database: {}", e.getMessage());
			}
		}
		
		logger.debug("Added {} channel(s) for logging in database", channelsToLog.size());

	}

	@Override
	public void log(List<LogRecordContainer> containers, long timestamp) {
		
		if(!initSuccessful) return;
				
		List<LogRecordContainer> doubleValues = new ArrayList<>();
		List<LogRecordContainer> longValues = new ArrayList<>();
		List<LogRecordContainer> intValues = new ArrayList<>();
		List<LogRecordContainer> boolValues = new ArrayList<>();
		List<LogRecordContainer> stringValues = new ArrayList<>();
		
		for (LogRecordContainer logRecordContainer : containers) {
			
			if(channelsToLog.containsKey(logRecordContainer.getChannelId())) {

				LogChannel channel = channelsToLog.get(logRecordContainer.getChannelId());
				switch (channel.getValueType()) {
					case LONG:
						longValues.add(logRecordContainer);
						break;
					case INTEGER:
					case SHORT:
					case BYTE:
						intValues.add(logRecordContainer);
						break;
					case BOOLEAN:
						boolValues.add(logRecordContainer);
						break;
					case BYTE_ARRAY:
					case STRING:
						stringValues.add(logRecordContainer);
						break;
				default:
					doubleValues.add(logRecordContainer);
					break;
				}
			}
		}
			
		try {
			if(!longValues.isEmpty()) database.logLongValues(longValues, timestamp);
			if(!intValues.isEmpty()) database.logIntValues(intValues, timestamp);
			if(!boolValues.isEmpty()) database.logBoolValues(boolValues, timestamp);
			if(!stringValues.isEmpty()) database.logStringValues(stringValues, timestamp);
			if(!doubleValues.isEmpty()) database.logDoubleValues(doubleValues, timestamp);
			
		} catch (SQLException e) {
			logger.error("Error while writing log values to database: {}", e.getMessage());
		}
		
	}

	@Override
	public List<Record> getRecords(String channelId, long startTime, long endTime) throws IOException {
		
		if(!initSuccessful) throw new IOException("Database initialization wasn't successful, cannot retrieve data");
		
		try {
			return database.getRecords(channelId, startTime, endTime);
		} catch (SQLException e) {
			throw new IOException("Error while retriving data from database", e);
		}
	}
	
}
