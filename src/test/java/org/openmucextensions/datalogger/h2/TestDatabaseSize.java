package org.openmucextensions.datalogger.h2;

import java.util.ArrayList;
import java.util.List;

import org.openmuc.framework.core.datamanager.LogRecordContainerImpl;
import org.openmuc.framework.data.DoubleValue;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.data.ValueType;
import org.openmuc.framework.datalogger.spi.LogChannel;
import org.openmuc.framework.datalogger.spi.LogRecordContainer;

public class TestDatabaseSize {

	public static void main(String[] args) {
		
		int recordCount = 1000000;
		
		H2Logger logger = new H2Logger();
		logger.activate(null);
		
		List<LogChannel> channelsToLog = new ArrayList<>();
		LogChannel channel = new LogChannel() {
			
			@Override
			public Integer getValueTypeLength() {
				return 0;
			}
			
			@Override
			public ValueType getValueType() {
				return ValueType.DOUBLE;
			}
			
			@Override
			public String getUnit() {
				return "K";
			}
			
			@Override
			public Integer getLoggingTimeOffset() {
				return 0;
			}
			
			@Override
			public Integer getLoggingInterval() {
				return 1;
			}
			
			@Override
			public String getId() {
				return "channelId";
			}
			
			@Override
			public String getDescription() {
				return "description";
			}
		};
		
		channelsToLog.add(channel);
		
		logger.setChannelsToLog(channelsToLog);
		
		ArrayList<LogRecordContainer> container = new ArrayList<>();
		
		for(int i=0; i<recordCount; i++) {
			container.clear();
			LogRecordContainer log = new LogRecordContainerImpl("channelId", new Record(new DoubleValue(i), (long) i));
			container.add(log);
			logger.log(container, (long) i); 
		}
		
		logger.deactivate(null);

	}

}
