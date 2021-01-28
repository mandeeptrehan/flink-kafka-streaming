package com.cloudera.flink.scala.bean

import scala.beans.BeanProperty

@SerialVersionUID(1L)
case class CovidBean (

  @BeanProperty var location: String,

  @BeanProperty var country_code: String,

  @BeanProperty var latitude: Float, 

  @BeanProperty var longitude: Float,

  @BeanProperty var confirmed: Int,

  @BeanProperty var dead: Int,

  @BeanProperty var recovered: Int,

  @BeanProperty var velocity_confirmed: Int,
  
  @BeanProperty var velocity_dead: Int,

  @BeanProperty var velocity_recovered: Int,

  @BeanProperty var updated_date: String,

  @BeanProperty var eventTimeLong: Long 
)
extends Serializable
