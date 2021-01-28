package com.cloudera.flink.filter;

import java.io.Serializable;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;

/* This class filters duplicates that occur within a configurable time of each other in a data stream.
 */
public class DedupeFilterValueState<T extends Serializable> extends RichFilterFunction<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	ValueState<Boolean> keyHasBeenSeen;

	public DedupeFilterValueState() {
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		
		StateTtlConfig ttlConfig = StateTtlConfig
			    .newBuilder(Time.seconds(100))
			    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
			    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
			    .build();
		
		ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
		
		stateDescriptor.enableTimeToLive(ttlConfig);
		
        keyHasBeenSeen = getRuntimeContext().getState(stateDescriptor);
	}

	@Override
	public boolean filter(T value) throws Exception {
		
		if ( keyHasBeenSeen.value() == null) {
            keyHasBeenSeen.update(true);
            return true;
        } else {
        	return false;
        }
		
	}

}