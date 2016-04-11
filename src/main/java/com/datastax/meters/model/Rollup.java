package com.datastax.meters.model;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.math.*;

public class Rollup {

	private String deviceID;
	private Date metricDay;
	private String metricName;
	private BigDecimal metricAvg;
	private BigDecimal metricMin;
	private BigDecimal metricMax;

	public Rollup() {
		super();
	}

	public String getDeviceID() {
		return deviceID;
	}

	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}

	public Date getMetricDay() {
		return metricDay;
	}

	public void setMetricDay(Date metricDay) {
		this.metricDay = metricDay;
	}

	public BigDecimal getMetricAvg() {
		return metricAvg;
	}

	public void setMetricAvg(BigDecimal metricAvg) {
		this.metricAvg = metricAvg;
	}
	public BigDecimal getMetricMin() {
		return metricMin;
	}

	public void setMetricMin(BigDecimal metricMin) {
		this.metricMin = metricMin;
	}
	public BigDecimal getMetricMax() {
		return metricMax;
	}

	public void setMetricMax(BigDecimal metricMax) {
		this.metricMax = metricMax;
	}

	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public String toString() {
		return "Rollup [deviceID=" + deviceID + ", metricDay=" + metricDay + ", metricName="
	+ metricName + ", metricAvg=" + metricAvg + "]"
	 + ", metricMin=" + metricMin + "]"
	 + ", metricMax=" + metricMax + "]";
	}

}
