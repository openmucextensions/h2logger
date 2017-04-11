package org.openmucextensions.datalogger.h2;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This task deletes old records in the database.
 * 
 * @author Mike Pichler
 *
 */
public class CleanupTask extends TimerTask {

	private static Logger logger = LoggerFactory.getLogger(H2Logger.class);
	
	private final DatabaseWrapper database;
	private final long storageInterval;
	
	public CleanupTask(final DatabaseWrapper database, long storageInterval) {
		super();
		this.database = database;
		this.storageInterval = storageInterval;
	}
	
	@Override
	public void run() {
		
		long threshold = System.currentTimeMillis() - storageInterval;
		
		try {
			int rowsAffected = database.deleteRecordsBefore(threshold);
			logger.debug("Deleted records before {} from database ({} record(s) affected)", new Timestamp(threshold).toString(), rowsAffected);
		} catch (SQLException e) {
			logger.error("Error while deleting old records from database: {}", e.getMessage());
		}
		
	}

}
