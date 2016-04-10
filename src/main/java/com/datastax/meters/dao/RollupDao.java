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
import com.datastax.meters.model.Rollup;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class RollupDao {

	private static Logger logger = LoggerFactory.getLogger(RollupDao.class);
	private Session session;

        /* change if needed - keyspace name and table name */
	private static String keyspaceName = "metrics";

	private static String rollupTable = keyspaceName + ".daily_rollups";
  
        /* use to select without time restriction*/
	private static final String GET_ROLLUPS_BY_ID = "select * from " + rollupTable
			+ " where device_id = ? ";

	private static final String GET_ROLLUPS_BY_TIME = "select * from " + rollupTable
			+ " where device_id = ? and metric_day >= ? and metric_day < ?";

	
	private PreparedStatement getRollupById;
	private PreparedStatement getRollupByTime;

	private AtomicLong count = new AtomicLong(0);

	public RollupDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();

		try {

			this.getRollupById = session.prepare(GET_ROLLUPS_BY_ID);
			this.getRollupByTime = session.prepare(GET_ROLLUPS_BY_TIME);


		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public Rollup getRollup(String deviceId) {

                /* changed if needed to reflect field names */
		ResultSetFuture rs = this.session.executeAsync(this.getRollupById.bind(deviceId));

		Row row = rs.getUninterruptibly().one();
		if (row == null) {
			throw new RuntimeException("Error - no rollup for id:" + deviceId);
		}

		return rowToRollup(row);
	}

	private Rollup rowToRollup(Row row) {

		Rollup t = new Rollup();

		t.setDeviceID(row.getString("device_id"));
		t.setMetricDay(row.getDate("metric_day"));
		t.setMetricName(row.getString("metric_name"));
		t.setMetricAvg(row.getDecimal("metric_avg"));
		t.setMetricMin(row.getDecimal("metric_min"));
		t.setMetricMax(row.getDecimal("metric_max"));

		return t;
	}

	
	public List<Rollup> getRollupsForDeviceIDAndDate(String deviceID, DateTime from,
			DateTime to) {

		ResultSet resultSet = this.session.execute(getRollupByTime.bind(deviceID, from.toDate(), to.toDate()));
		
		return processResultSet(resultSet);
	}

	private List<Rollup> processResultSet(ResultSet resultSet ) {
		List<Row> rows = resultSet.all();
		List<Rollup> rollups = new ArrayList<Rollup>();

		for (Row row : rows) {

			Rollup rollup = rowToRollup(row);	
			
		        rollups.add(rollup);
		}
		return rollups;
	}
}
