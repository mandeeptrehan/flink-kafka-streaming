package com.cloudera.flink.scala.filter

import java.io.Serializable

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor

@SerialVersionUID(1L)
class DedupeFilterValueState[T <: Serializable] extends RichFilterFunction[T] {

  var keyHasBeenSeen: ValueState[java.lang.Boolean] = _

  override def open(parameters: Configuration): Unit = {

    val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(100))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    val stateDescriptor = new ValueStateDescriptor("keyHasBeenSeen", Types.BOOLEAN)
    stateDescriptor.enableTimeToLive(ttlConfig)
    keyHasBeenSeen = getRuntimeContext.getState(stateDescriptor)
  }

  override def filter(value: T): Boolean =
    if (keyHasBeenSeen.value() == null) {
      keyHasBeenSeen.update(true)
      return true
    } else {
      return false
    }

}
