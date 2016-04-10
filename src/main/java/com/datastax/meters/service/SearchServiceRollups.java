package com.datastax.meters.service;

import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.datastax.meters.model.Metric;
import com.datastax.meters.model.Rollup;

public interface SearchServiceRollups {

	public double getTimerAvg();

	List<Rollup> getRollupsByIDAndDate(String DeviceID, DateTime from, DateTime to);
}
