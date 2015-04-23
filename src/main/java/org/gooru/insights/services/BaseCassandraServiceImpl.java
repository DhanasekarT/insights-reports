package org.gooru.insights.services;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.gooru.insights.constants.CassandraConstants;
import org.gooru.insights.constants.ErrorConstants;
import org.gooru.insights.exception.handlers.ReportGenerationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;

@Component
public class BaseCassandraServiceImpl implements BaseCassandraService {

	@Autowired
	private BaseConnectionService connector;

	@Autowired
	private BaseAPIService baseAPIService;

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	private static final int SLEEP_TIME_MS = 2000;

	private static final int LOOP_COUNT = 5;

	public OperationResult<ColumnList<String>> readColumns(String keyspace, String columnFamilyName, String rowKey, Collection<String> columns) {

		try {
			RowQuery<String, String> rowQuery = connector.connectInsights().prepareQuery(accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(SLEEP_TIME_MS, LOOP_COUNT)).getKey(rowKey);
			if (baseAPIService.checkNull(columns)) {
				rowQuery.withColumnSlice(columns);
			}
			return rowQuery.execute();
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}
	}

	public OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamilyName, Collection<String> keys, Collection<String> columns) {

		OperationResult<Rows<String, String>> queryResult = null;
		try {
			RowSliceQuery<String, String> Query = connector.connectInsights().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(SLEEP_TIME_MS, LOOP_COUNT)).getKeySlice(keys);
			if (!columns.isEmpty()) {
				Query.withColumnSlice(columns);
			}
			queryResult = Query.execute();
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}
		return queryResult;

	}

	public ColumnList<String> read(String keyspace, String cfName, String key) {
		ColumnList<String> result = null;
		try {
			result = connector.connectInsights().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(SLEEP_TIME_MS, LOOP_COUNT)).getKey(key).execute().getResult();

		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}

		return result;
	}

	public Column<String> readColumnValue(String keyspace, String cfName, String key, String columnName) {
		ColumnList<String> result = null;
		try {
			result = connector.connectInsights().prepareQuery(this.accessColumnFamily(cfName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(SLEEP_TIME_MS, LOOP_COUNT)).getKey(key).execute().getResult();

		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}
		if (!StringUtils.isBlank(columnName)) {
			return result.getColumnByName(columnName);
		}
		return null;
	}

	public void saveStringValue(String keyspace, String cfName, String key, String columnName, String value) {

		MutationBatch m = connector.connectInsights().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(SLEEP_TIME_MS, LOOP_COUNT));
		m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);
		try {
			m.execute();
		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}
	}

	public OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamily, Collection<String> columns) {
		OperationResult<Rows<String, String>> queryResult = null;
		AllRowsQuery<String, String> allRowQuery = null;
		try {
			allRowQuery = connector.connectInsights().prepareQuery(this.accessColumnFamily(columnFamily)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getAllRows();
			if (!columns.isEmpty()) {
				allRowQuery.withColumnSlice(columns);
			}
			queryResult = allRowQuery.execute();

		} catch (Exception e) {
			throw new ReportGenerationException(ErrorConstants.QUERY_ERROR + e);
		}
		return queryResult;
	}

	private ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> columnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
		return columnFamily;
	}
}
