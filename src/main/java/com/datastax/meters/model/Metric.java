package com.datastax.meters.model;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.math.*;

public class Metric {

	private String deviceID;
	private Date metricTime;
	private String metricName;
	private BigDecimal metricValue;

	public Metric() {
		super();
	}

	public String getDeviceID() {
		return deviceID;
	}

	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}

	public Date getMetricTime() {
		return metricTime;
	}

	public void setMetricTime(Date metricTime) {
		this.metricTime = metricTime;
	}

	public BigDecimal getMetricValue() {
		return metricValue;
	}

	public void setMetricValue(BigDecimal metricValue) {
		this.metricValue = metricValue;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public String toString() {
		return "Metric [deviceID=" + deviceID + ", metricTime=" + metricTime + ", metricName="
				+ metricName + ", metricValue=" + metricValue + "]";
	}

}
