package com.cloudera.flink.bean;

import java.io.Serializable;

public class CovidBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String location;
	private String country_code;
	private float latitude;
	private float longitude;
	private int confirmed;
	private int dead;
	private int recovered;
	private int velocity_confirmed;
	private int velocity_dead;
	private int velocity_recovered;
	private String updated_date;
	private long eventTimeLong;
	
	public CovidBean() {}
	
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getCountry_code() {
		return country_code;
	}
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}
	public float getLatitude() {
		return latitude;
	}
	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}
	public float getLongitude() {
		return longitude;
	}
	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}
	public int getConfirmed() {
		return confirmed;
	}
	public void setConfirmed(int confirmed) {
		this.confirmed = confirmed;
	}
	public int getDead() {
		return dead;
	}
	public void setDead(int dead) {
		this.dead = dead;
	}
	public int getRecovered() {
		return recovered;
	}
	public void setRecovered(int recovered) {
		this.recovered = recovered;
	}
	public int getVelocity_confirmed() {
		return velocity_confirmed;
	}
	public void setVelocity_confirmed(int velocity_confirmed) {
		this.velocity_confirmed = velocity_confirmed;
	}
	public int getVelocity_dead() {
		return velocity_dead;
	}
	public void setVelocity_dead(int velocity_dead) {
		this.velocity_dead = velocity_dead;
	}
	public int getVelocity_recovered() {
		return velocity_recovered;
	}
	public void setVelocity_recovered(int velocity_recovered) {
		this.velocity_recovered = velocity_recovered;
	}
	public String getUpdated_date() {
		return updated_date;
	}
	public void setUpdated_date(String updated_date) {
		this.updated_date = updated_date;
	}
	public long getEventTimeLong() {
		return eventTimeLong;
	}
	public void setEventTimeLong(long eventTimeLong) {
		this.eventTimeLong = eventTimeLong;
	}

	@Override
	public String toString() {
		return "CovidBean [location=" + location + ", country_code=" + country_code + ", latitude=" + latitude
				+ ", longitude=" + longitude + ", confirmed=" + confirmed + ", dead=" + dead + ", recovered="
				+ recovered + ", velocity_confirmed=" + velocity_confirmed + ", velocity_dead=" + velocity_dead
				+ ", velocity_recovered=" + velocity_recovered + ", updated_date=" + updated_date + ", eventTimeLong="
				+ eventTimeLong + "]";
	}

}
