package org.openmucextensions.datalogger.h2;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;

import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.openmuc.framework.data.BooleanValue;
import org.openmuc.framework.data.DoubleValue;
import org.openmuc.framework.data.Flag;
import org.openmuc.framework.data.FloatValue;
import org.openmuc.framework.data.LongValue;
import org.openmuc.framework.data.Record;
import org.openmuc.framework.data.StringValue;
import org.openmuc.framework.data.ValueType;
import org.openmuc.framework.datalogger.spi.LogChannel;
import org.openmuc.framework.datalogger.spi.LogRecordContainer;

public class TestH2Logger {

	H2Logger instance;
	
	@Before
	public void setUp() throws Exception {
		instance = new H2Logger();
		instance.activate(null);
	}

	@After
	public void tearDown() throws Exception {
		instance.deactivate(null);
		new File("./h2logger/database.mv.db").delete();
		new File("./h2logger").delete();
	}

	@Test
	public void testGetId() {
		assertThat(instance.getId(), is("h2logger"));
	}
	
	@Test
	public void testDoubleLogging() throws Throwable {
		
		final String channelId = "doubleChannel";
		double value = 10.0;
		long timestamp = 1000l;
				
		instance.setChannelsToLog(getLogChannelList(channelId, ValueType.DOUBLE));
		
		Record record = new Record(new DoubleValue(value), timestamp, Flag.VALID);
		instance.log(getLogRecordContainerList(channelId, record), timestamp);
		
		List<Record> records = instance.getRecords(channelId, timestamp, timestamp);
		
		assertThat(records.size(), is(1));
		assertThat(records.get(0).getValue().asDouble(), is(value));
		
	}
		
	@Test
	public void testStringLogging() throws Throwable {
		
		final String channelId = "stringChannel";
		String value = "test - string";
		long timestamp = 2000l;
				
		instance.setChannelsToLog(getLogChannelList(channelId, ValueType.STRING));
		
		Record record = new Record(new StringValue(value), timestamp, Flag.VALID);
		instance.log(getLogRecordContainerList(channelId, record), timestamp);
		
		List<Record> records = instance.getRecords(channelId, timestamp, timestamp);
		
		assertThat(records.size(), is(1));
		assertThat(records.get(0).getValue().asString(), is(value));
		
	}
	
	@Test
	public void testFloatLogging() throws Throwable {
		
		final String channelId = "floatChannel";
		float value = 10.0f;
		long timestamp = 3000l;
				
		instance.setChannelsToLog(getLogChannelList(channelId, ValueType.FLOAT));
		
		Record record = new Record(new FloatValue(value), timestamp, Flag.VALID);
		instance.log(getLogRecordContainerList(channelId, record), timestamp);
		
		List<Record> records = instance.getRecords(channelId, timestamp, timestamp);
		
		assertThat(records.size(), is(1));
		assertThat(records.get(0).getValue().asFloat(), is(value));
		
	}
	
	@Test
	public void testLongLogging() throws Throwable {
		
		final String channelId = "longChannel";
		long value = 10l;
		long timestamp = 4000l;
				
		instance.setChannelsToLog(getLogChannelList(channelId, ValueType.LONG));
		
		Record record = new Record(new LongValue(value), timestamp, Flag.VALID);
		instance.log(getLogRecordContainerList(channelId, record), timestamp);
		
		List<Record> records = instance.getRecords(channelId, timestamp, timestamp);
		
		assertThat(records.size(), is(1));
		assertThat(records.get(0).getValue().asLong(), is(value));
		
	}
	
	@Test
	public void testBoolLogging() throws Throwable {
		
		final String channelId = "longChannel";
		boolean value = true;
		long timestamp = 5000l;
				
		instance.setChannelsToLog(getLogChannelList(channelId, ValueType.BOOLEAN));
		
		Record record = new Record(new BooleanValue(value), timestamp, Flag.VALID);
		instance.log(getLogRecordContainerList(channelId, record), timestamp);
		
		List<Record> records = instance.getRecords(channelId, timestamp, timestamp);
		
		assertThat(records.size(), is(1));
		assertThat(records.get(0).getValue().asBoolean(), is(value));
		
	}
	
	private List<LogChannel> getLogChannelList(String channelId, ValueType type) {
		
		LogChannel channel = mock(LogChannel.class);
		when(channel.getId()).thenReturn(channelId);
		when(channel.getValueType()).thenReturn(type);
		
		List<LogChannel> channels = new ArrayList<>();
		channels.add(channel);
		
		return channels;
	}
	
	private List<LogRecordContainer> getLogRecordContainerList(String channelId, Record record) {
		
		LogRecordContainer container = mock(LogRecordContainer.class);
		when(container.getChannelId()).thenReturn(channelId);
		when(container.getRecord()).thenReturn(record);
		
		List<LogRecordContainer> containers = new ArrayList<>();
		containers.add(container);
		
		return containers;
	}

}
