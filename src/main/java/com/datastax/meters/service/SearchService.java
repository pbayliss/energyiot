package com.datastax.meters.service;

import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.datastax.meters.model.Metric;

public interface SearchService {

	public double getTimerAvg();

	List<Metric> getMetricsByIDAndDate(String DeviceID, DateTime from, DateTime to);
}
