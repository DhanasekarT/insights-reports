package org.gooru.insights.services;

import java.util.Collection;

import org.gooru.insights.constants.CassandraConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;

@Service
public class BaseCassandraServiceImpl implements BaseCassandraService,CassandraConstants{

	@Autowired
	BaseConnectionService connector;
	
	@Autowired
	BaseAPIService baseAPIService;

	 protected static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;
	 
	
	public OperationResult<ColumnList<String>> readColumns(String keyspace, String columnFamilyName, String rowKey,Collection<String> columns) {

			Keyspace queryKeyspace = null;
			if (keyspaces.INSIGHTS.keyspace().equalsIgnoreCase(keyspace)) {
				queryKeyspace = connector.connectInsights();
			} else {
				queryKeyspace = connector.connectSearch();
			}
			try {
				RowQuery<String, String> rowQuery = queryKeyspace.prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKey(rowKey);
			
				if(baseAPIService.checkNull(columns)){
					rowQuery.withColumnSlice(columns);
				}
				return rowQuery.execute();
			} catch (ConnectionException e) {

				e.printStackTrace();
				System.out.println("Query execution exeption");
			}
			return null;

		}
	
	public OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamilyName, Collection<String> keys, Collection<String> columns) {

		OperationResult<Rows<String, String>> queryResult = null;

		Keyspace queryKeyspace = null;
		if (keyspaces.INSIGHTS.keyspace().equalsIgnoreCase(keyspace)) {
			queryKeyspace = connector.connectInsights();
		} else {
			queryKeyspace = connector.connectSearch();
		}
		try {
			RowSliceQuery<String, String> Query = queryKeyspace.prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getKeySlice(keys);
			if (!columns.isEmpty()) {
				Query.withColumnSlice(columns);
			}
			queryResult = Query.execute();

		} catch (ConnectionException e) {

			e.printStackTrace();
			System.out.println("Query execution exeption");
		}
		return queryResult;

	}
	
	public OperationResult<Rows<String, String>> readAll(String keyspace, String columnFamily,Collection<String> columns) {
		OperationResult<Rows<String, String>> queryResult = null;
		AllRowsQuery<String, String> allRowQuery = null;
		Keyspace queryKeyspace = null;
		if (keyspaces.INSIGHTS.keyspace().equalsIgnoreCase(keyspace)) {
			queryKeyspace = connector.connectInsights();
		} else {
			queryKeyspace = connector.connectSearch();
		}
		try {

			allRowQuery  = queryKeyspace.prepareQuery(this.accessColumnFamily(columnFamily)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).getAllRows();

			if (!columns.isEmpty()) {
				allRowQuery.withColumnSlice(columns);
			}
			
			queryResult = allRowQuery.execute();

		} catch (ConnectionException e) {
			e.printStackTrace();
			System.out.println("Query execution exeption");
		}
		return queryResult;
	}
	
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> columnFamily;

		columnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

		return columnFamily;
	}
}

