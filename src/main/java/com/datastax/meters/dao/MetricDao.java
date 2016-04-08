package com.datastax.meters.dao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.meters.model.Metric;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class MetricDao {

	private static Logger logger = LoggerFactory.getLogger(MetricDao.class);
	private Session session;

        /* change if needed - keyspace name and table name */
	private static String keyspaceName = "metrics";

	private static String metricTable = keyspaceName + ".raw_metrics";
  
        /* use to select without time restriction*/
	private static final String GET_METRICS_BY_ID = "select * from " + metricTable
			+ " where device_id = ? ";

	private static final String GET_METRICS_BY_TIME = "select * from " + metricTable
			+ " where device_id = ? and metric_time >= ? and metric_time < ?";

	
	private PreparedStatement getMetricById;
	private PreparedStatement getMetricByTime;

	private AtomicLong count = new AtomicLong(0);

	public MetricDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();

		try {

			this.getMetricById = session.prepare(GET_METRICS_BY_ID);
			this.getMetricByTime = session.prepare(GET_METRICS_BY_TIME);


		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public Metric getMetric(String deviceId) {

                /* changed if needed to reflect field names */
		ResultSetFuture rs = this.session.executeAsync(this.getMetricById.bind(deviceId));

		Row row = rs.getUninterruptibly().one();
		if (row == null) {
			throw new RuntimeException("Error - no metric for id:" + deviceId);
		}

		return rowToMetric(row);
	}

	private Metric rowToMetric(Row row) {

		Metric t = new Metric();

		t.setDeviceID(row.getString("device_id"));
		t.setMetricTime(row.getDate("metric_time"));
		t.setMetricName(row.getString("metric_name"));
		t.setMetricValue(row.getDecimal("metric_value"));

		return t;
	}

	
	public List<Metric> getMetricsForDeviceIDAndDate(String deviceID, DateTime from,
			DateTime to) {

		ResultSet resultSet = this.session.execute(getMetricByTime.bind(deviceID, from.toDate(), to.toDate()));
		
		return processResultSet(resultSet);
	}

	private List<Metric> processResultSet(ResultSet resultSet ) {
		List<Row> rows = resultSet.all();
		List<Metric> metrics = new ArrayList<Metric>();

		for (Row row : rows) {

			Metric metric = rowToMetric(row);	
			
		        metrics.add(metric);
		}
		return metrics;
	}
}
