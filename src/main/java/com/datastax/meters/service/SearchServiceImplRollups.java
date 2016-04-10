package com.datastax.meters.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;

import com.datastax.meters.dao.MetricDao;
import com.datastax.meters.dao.RollupDao;
import com.datastax.meters.model.Metric;
import com.datastax.meters.model.Rollup;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class SearchServiceImplRollups implements SearchServiceRollups {

	private RollupDao dao;
	private long timerSum = 0;
	private AtomicLong timerCount= new AtomicLong();

	public SearchServiceImplRollups() {		
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		this.dao = new RollupDao(contactPointsStr.split(","));
	}	

	@Override
	public double getTimerAvg(){
		return timerSum/timerCount.get();
	}

	@Override

	public List<Rollup> getRollupsByIDAndDate(String deviceID,  DateTime from, DateTime to) {
		
		Timer timer = new Timer();
		List<Rollup> rollups;

		rollups = dao.getRollupsForDeviceIDAndDate(deviceID, from, to);
			
		timer.end();
		timerSum += timer.getTimeTakenMillis();
		timerCount.incrementAndGet();
		return rollups;
	}
}
