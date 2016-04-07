package com.datastax.meters.dao;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.meters.model.Transaction;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class TransactionDao {

	private static Logger logger = LoggerFactory.getLogger(TransactionDao.class);
	private Session session;

        /* change if needed - keyspace name and table name */
	private static String keyspaceName = "metrics";

	private static String transactionTable = keyspaceName + ".raw_metrics";
  
        /* use to select without time restriction*/
	private static final String GET_TRANSACTIONS_BY_ID = "select * from " + transactionTable
			+ " where device_id = ? ";

	private static final String GET_TRANSACTIONS_BY_TIME = "select * from " + transactionTable
			+ " where device_id = ? and metric_time >= ? and metric_time < ?";

	
	private PreparedStatement getTransactionById;
	private PreparedStatement getTransactionByTime;

	private AtomicLong count = new AtomicLong(0);

	public TransactionDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();

		this.session = cluster.connect();

		try {

			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);
			this.getTransactionByTime = session.prepare(GET_TRANSACTIONS_BY_TIME);


		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public Transaction getTransaction(String deviceId) {

                /* changed if needed to reflect field names */
		ResultSetFuture rs = this.session.executeAsync(this.getTransactionById.bind(deviceId));

		Row row = rs.getUninterruptibly().one();
		if (row == null) {
			throw new RuntimeException("Error - no transaction for id:" + deviceId);
		}

		return rowToTransaction(row);
	}

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();

		t.setDeviceID(row.getString("device_id"));
		t.setMetricTime(row.getDate("metric_time"));
		t.setMetricName(row.getString("metric_name"));
		t.setMetricValue(row.getDecimal("metric_value"));

		return t;
	}

	
	public List<Transaction> getTransactionsForDeviceIDTagsAndDate(String deviceID, DateTime from,
			DateTime to) {

		ResultSet resultSet = this.session.execute(getTransactionByTime.bind(deviceID, from.toDate(), to.toDate()));
		
		return processResultSet(resultSet);
	}

	private List<Transaction> processResultSet(ResultSet resultSet ) {
		List<Row> rows = resultSet.all();
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows) {

			Transaction transaction = rowToTransaction(row);	
			
		        transactions.add(transaction);
		}
		return transactions;
	}
}
