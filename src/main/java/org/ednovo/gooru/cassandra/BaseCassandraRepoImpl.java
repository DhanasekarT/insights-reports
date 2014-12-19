package org.ednovo.gooru.cassandra;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

	@Repository
	public class BaseCassandraRepoImpl extends BaseDAOCassandraImpl implements Constants,BaseCassandraRepo {
		
		private static CassandraConnectionProvider connectionProvider;
		
		private static final Logger logger = LoggerFactory.getLogger(BaseCassandraRepoImpl.class);

		private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		
		/*@Autowired
		SchdulerService schdulerService;*/
		
		public BaseCassandraRepoImpl(CassandraConnectionProvider connectionProvider) {
			super(connectionProvider);
		}

		public BaseCassandraRepoImpl() {
			super(connectionProvider);
		}

		public Column<String> readWithKeyColumn(String cfName,String key,String columnName){
			return this.readWithKeyColumn(cfName, key, columnName, null);
		}
		
	    public Column<String> readWithKeyColumn(String cfName,String key,String columnName, String keySpaceType){
	    	Keyspace keySpace = getKeyspace();
			if (keySpaceType != null && keySpaceType.equalsIgnoreCase("AWS")) {
				keySpace = getAwsKeyspace();
			}
	    	Column<String> result = null;
	    	try {
	              result = keySpace.prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKey(key)
	                    .getColumn(columnName)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	        	return null;
	        }
	    	
	    	return result;
	    }

	    public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList){
	    	return this.readWithKeyColumnList(cfName, key, columnList, null);
	    }
	    
	    public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList, String keySpaceType){
	    	Keyspace keySpace = getKeyspace();
			if (keySpaceType != null && keySpaceType.equalsIgnoreCase("AWS")) {
				keySpace = getAwsKeyspace();
			}
	    	ColumnList<String> result = null;
	    	try {
	              result = keySpace.prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKey(key)
	                    .withColumnSlice(columnList)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKeyColumnList {} ", e);
	        }
	    	
	    	return result;
	    }
	    
	    public Rows<String, String> readWithKeyListColumnList(String cfName,Collection<String> keys,Collection<String> columnList){
	        
	    	Rows<String, String> result = null;
	    	try {
	              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKeySlice(keys)
	                    .withColumnSlice(columnList)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKeyListColumnList {} ", e);
	        }
	    	
	    	return result;
	    }
	    public ColumnList<String> readWithKey(String cfName,String key){
	        
	    	ColumnList<String> result = null;
	    	try {
	              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKey(key)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKey {} ", e);
	        }
	    	
	    	return result;
	    }
	    
	    public Rows<String, String> readWithKeyList(String cfName,Collection<String> key){
	        
	    	Rows<String, String> result = null;
	    	try {
	              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKeySlice(key)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKey {}", e);
	        }
	    	
	    	return result;
	    }

	    public Rows<String, String> readCommaKeyList(String cfName,String... key){

	    	Rows<String, String> result = null;
	    	try {
	              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKeySlice(key)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKey {}", e);
	        }
	    	
	    	return result;
	    }
	    
	    public Rows<String, String> readIterableKeyList(String cfName,Iterable<String> keys){

	    	Rows<String, String> result = null;
	    	try {
	              result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	                    .getKeySlice(keys)
	                    .execute()
	                    .getResult()
	                    ;

	        } catch (Exception e) {
	            logger.error("Error while fetching data from method : readWithKey {} ", e);
	        }
	    	
	    	return result;
	    }
	    
	    public Rows<String, String> readIndexedColumn(String cfName,String columnName,String value){
	    	
	    	Rows<String, String> result = null;
	    	try{
	    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex()
				.addExpression()
				.whereColumn(columnName)
				.equals()
				.value(value)
				.execute()
				.getResult()
				;
		    	
	    	} catch(Exception e){
	    		logger.error("Error while fetching data from method : readIndexedColumn {} ", e);    		
	    	}
	    	return result;
	    }
	    
	    public Rows<String, String> readIndexedColumn(String cfName,String columnName,long value){
	    	
	    	Rows<String, String> result = null;
	    	try{
	    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex()
				.addExpression()
				.whereColumn(columnName)
				.equals()
				.value(value)
				.execute()
				.getResult()
				;
		    	
	    	} catch(Exception e){
	    		logger.error("Error while fetching data from method : readIndexedColumn {} ", e);    		
	    	}
	    	return result;
	    }

 public Rows<String, String> readIndexedColumn(String cfName,String columnName,int value){
	    	
	    	Rows<String, String> result = null;
	    	try{
	    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
			 	.searchWithIndex()
				.addExpression()
				.whereColumn(columnName)
				.equals()
				.value(value)
				.execute()
				.getResult()
				;
		    	
	    	} catch(Exception e){
	    		logger.error("Error while fetching data from method : readIndexedColumn {} ", e);    		
	    	}
	    	return result;
	    }
 
	    public Rows<String, String> readIndexedColumnLastNrows(String cfName ,String columnName,String value, Integer rowsToRead) {
	    	
			Rows<String, String> result = null;
	    	try {
	    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				 			   .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
						 	   .searchWithIndex().autoPaginateRows(true)
						 	   .setRowLimit(rowsToRead.intValue())
						 	   .addExpression().whereColumn(columnName)
						 	   .equals().value(value).execute().getResult();
			} catch (ConnectionException e) {
				logger.error("Error while fetching data from method : readIndexedColumnLastNrows {} ", e);
			}
	    	
	    	return result;
		}

	    public ColumnList<String> readKeyLastNColumns(String cfName,String key, Integer columnsToRead) {
	    	
	    	ColumnList<String> result = null;
	    	try {
	    		result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	    		.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	    		.getKey(key)
	    		.withColumnRange(new RangeBuilder().setReversed().setLimit(columnsToRead.intValue()).build())
	    		.execute().getResult();
	    	} catch (ConnectionException e) {
	    		logger.error("Error while fetching data from method : readKeyLastNColumns {} ", e);
	    	}
	    	
	    	return result;
	    }
	    
	    public long getCount(String cfName,String key){

	    	Integer columns = null;
			
	    	try {
				columns = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
						.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
						.getKey(key)
						.getCount()
						.execute().getResult();
			} catch (ConnectionException e) {
				e.printStackTrace();
			}
			
			return columns.longValue();
		}
	    
	    public Rows<String, String> readIndexedColumnList(String cfName,Map<String,String> columnList){
	    	
	    	Rows<String, String> result = null;
	    	IndexQuery<String, String> query = null;

	    	query = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
	    				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
	    				.searchWithIndex();
	    	
	    	for (Map.Entry<String, String> entry : columnList.entrySet()) {
	    		query.addExpression().whereColumn(entry.getKey()).equals().value(entry.getValue())
	    	    ;
	    	}
	    	
	    	try{
	    		result = query.execute().getResult()
				;
	    	} catch(Exception e){
	    		logger.error("Error while fetching data from method : readIndexedColumnList {} ", e);    		
	    	}

	    	return result;
	    }
	    
	    public boolean isRowKeyExists(String cfName,String key) {

			try {
				return getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.getKey(key).execute()
					.getResult()
					.isEmpty();
			} catch (Exception e) {
				logger.error("Error while fetching data from method : isRowKeyExists {} ", e);
			}
			return false;
		}
	    
	    public boolean isValueExists(String cfName,Map<String,Object> columns) {

			try {
				IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).searchWithIndex();
				
				for(Map.Entry<String, Object> map : columns.entrySet()){
					cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
				}
				return cfQuery.execute().getResult().isEmpty();
			} catch (Exception e) {
				logger.error("Error while fetching data from method : isRowKeyExists {} ", e);
			}
			return false;
		}
	    
	    public Collection<String> getKey(String cfName,Map<String,Object> columns) {

			try {
				
				IndexQuery<String, String> cfQuery = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).searchWithIndex();
				
				for(Map.Entry<String, Object> map : columns.entrySet()){
					cfQuery.addExpression().whereColumn(map.getKey()).equals().value(map.getValue().toString());
				}
				Rows<String, String> rows = cfQuery.execute().getResult();
				return rows.getKeys();
			} catch (Exception e) {
				logger.error("Error while fetching data from method : isRowKeyExists {} ", e);
			}
			return new ArrayList();
		}
	    
		public ColumnList<String> readColumnsWithPrefix(String cfName,String rowKey, String startColumnNamePrefix, String endColumnNamePrefix, Integer rowsToRead){
			ColumnList<String> result = null;
			String startColumnPrefix = null;
			String endColumnPrefix = null;
			startColumnPrefix = startColumnNamePrefix+"~\u0000";
			endColumnPrefix = endColumnNamePrefix+"~\uFFFF";
		
			try {
				result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
				.getKey(rowKey) .withColumnRange(new RangeBuilder().setLimit(rowsToRead)
				.setStart(startColumnPrefix)
				.setEnd(endColumnPrefix)
				.build()).execute().getResult();
			} catch (Exception e) {
				logger.error("Error while fetching data from method : readColumnsWithPrefix {} ", e);
			} 
			
			return result;
		}
		
	    public Rows<String, String> readAllRows(String cfName){
	    	
	    	Rows<String, String> result = null;
			try {
				result = getKeyspace().prepareQuery(this.accessColumnFamily(cfName))
						.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
						.getAllRows()
						.withColumnRange(new RangeBuilder().build())
				        .setExceptionCallback(new ExceptionCallback() {
				        @Override
				        public boolean onException(ConnectionException e) {
				                 try {
				                     Thread.sleep(1000);
				                 } catch (InterruptedException e1) {
				                 }
				                 return true;
				             }})
				        .execute().getResult();
			} catch (Exception e) {
				logger.error("Error while fetching data from method : readAllRows {} ", e);
			}
			
			return result;
		}
	    
	    
	    public void saveBulkStringList(String cfName, String key,Map<String,String> columnValueList) {

	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        for (Map.Entry<String,String> entry : columnValueList.entrySet()) {
	    			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
	    	    ;
	    	}

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveBulkStringList {}", e);
	        }
	    }
	    //Save Muliple rows
	    public void saveListOfResource(String cfName, String key,List<Map<String,String>> resourceList) {
	    	System.out.println("Keyspace: ====>>>" + getKeyspace());;
	    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    	List<Map<String,String>> resources = resourceList;
	    	for (Map<String, String> resource : resources) {
				Map<String, String> treeResource = new TreeMap<String, String>(resource);
				for (Map.Entry<String, String> entry : treeResource.entrySet()) {
					ColumnListMutation<String> value = m.withRow(this.accessColumnFamily(cfName),treeResource.get(key));
					value.putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
				}
			}
	        try {
	        	OperationResult<Void> result = m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveBulkStringList {}", e);
	        }
	    }
	    
	/*    public void saveListData(String cfName, String key,List<Map<String,Object>> resourceList) {
	    	saveListData(cfName,key,resourceList,null);
	    }*/
	    
/*	    public void saveListData(String cfName, String key, List<Map<String, Object>> resourceList, String keySpaceType) {
			Keyspace keySpace = getKeyspace();
			int partitionSize = 2000;
			if (keySpaceType != null && keySpaceType.equalsIgnoreCase("AWS")) {
				keySpace = getAwsKeyspace();
			}
			try {
				Map<String, String> tableDatatype = schdulerService.getTablesDataTypeCache().get(cfName);
				for(List<Map<String,Object>> resourceSubList : Lists.partition(resourceList, partitionSize)){
					MutationBatch m = keySpace.prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
					for (Map<String, Object> resource : resourceSubList) {
						if (resource != null && !resource.isEmpty() && resource.containsKey(key) && StringUtils.trimToNull(resource.get(key).toString()) != null) {
							ColumnListMutation<String> value = m.withRow(this.accessColumnFamily(cfName), resource.get(key).toString());
							for (Map.Entry<String, Object> entry : resource.entrySet()) {
								if (tableDatatype.containsKey(entry.getKey())) {
									if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("int") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("integer")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? Integer.parseInt(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("string") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("text")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("long") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("bigint")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? Long.parseLong(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("boolean")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? Boolean.valueOf((entry.getValue().toString())) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("timestamp")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? dateFormatter.parse(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("date")) {
										value.putColumnIfNotNull(entry.getKey(), entry.getValue() != null ? (Date) entry.getValue() : null, null);
									}
								}
							}
						}
					}
					m.execute();
				}
			} catch (Exception e) {
				logger.error("Error while save in method : saveBulkListData {}" + e);
			}  
		}
*/	    
	    public void saveListOfResourceObject(String cfName, String key,List<Map<String,Object>> resourceList) {
	    	MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
	    	for (Map<String, Object> resource : resourceList) {
				Map<String, Object> treeResource = new TreeMap<String, Object>(resource);
				for (Map.Entry<String, Object> entry : treeResource.entrySet()) {
					if(treeResource.get(key) != null){
					ColumnListMutation<String> value = m.withRow(this.accessColumnFamily(cfName),treeResource.get(key).toString());
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("String")){
							value.putColumnIfNotNull(entry.getKey(), entry.getValue().toString(), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Long")){
							value.putColumnIfNotNull(entry.getKey(), Long.valueOf(entry.getValue().toString()), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Boolean")){
							value.putColumnIfNotNull(entry.getKey(), Boolean.valueOf(entry.getValue().toString()), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Timestamp")){
							value.putColumnIfNotNull(entry.getKey(), entry.getValue().toString(), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("BigInteger")){
							value.putColumnIfNotNull(entry.getKey(), Long.valueOf(entry.getValue().toString()), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Integer")){
							value.putColumnIfNotNull(entry.getKey(), Integer.parseInt(entry.getValue().toString()), null);
						}
						if(entry.getValue() != null && entry.getValue().getClass().getSimpleName().equalsIgnoreCase("Character")){
							value.putColumnIfNotNull(entry.getKey(), entry.getValue().toString(), null);
						}
					}
				}
			}
	        try {
	        	m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveBulkStringList {}"+e);
	        }
	    }
	    
	    public void saveBulkLongList(String cfName, String key,Map<String,Long> columnValueList) {

	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        for (Map.Entry<String,Long> entry : columnValueList.entrySet()) {
	    			m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(entry.getKey(), entry.getValue(), null);
	    	    ;
	    	}

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveBulkLongList {}", e);
	        }
	    }
	    
	    public void saveStringValue(String cfName, String key,String columnName,String value) {

	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveStringValue {}", e);
	        }
	    }
	    
	    public void saveIntegerValue(String cfName, String key,String columnName,Integer value){
	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.error("Error while save in method : saveIntegerValue {}", e);
	        }
	    	
	    }

	    public void saveMultipleStringValue(String cfName, String key,Map<String,Object> columns, String keySpaceType) {

	    	Keyspace keyspace = getKeyspace();
	    	if(keySpaceType != null && keySpaceType.equalsIgnoreCase("AWS")){
	    		keyspace = getAwsKeyspace();
	    	}
	        MutationBatch m = keyspace.prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        for(Map.Entry<String, Object> column : columns.entrySet()){
	        	m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(column.getKey(), column.getValue().toString(), null);
	    	}
	        
	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("Error while save in method : saveMultipleStringValue {}", e);
	        }
	    }

	    public void saveLongValue(String cfName, String key,String columnName,long value) {

	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        m.withRow(this.accessColumnFamily(cfName), key).putColumnIfNotNull(columnName, value, null);

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("Error while save in method : saveLongValue {}", e);
	        }
	    }
	    
	    public void increamentCounter(String cfName, String key,String columnName,long value) {

	        MutationBatch m = getKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);

	        m.withRow(this.accessColumnFamily(cfName), key)
	        .incrementCounterColumn(columnName, value);

	        try {
	            m.execute();
	        } catch (ConnectionException e) {
	            logger.info("Error while save in method : saveLongValue {}", e);
	        }
	    }
	    
	    public void generateCounter(String cfName,String key,String columnName, long value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .incrementCounterColumn(columnName, value);
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, String value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value);
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, long value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value);
	    }
	    
	    public void generateTTLColumns(String cfName,String key,String columnName, long value,int expireTime ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value,expireTime);
	    }
	    public void generateTTLColumns(String cfName,String key,String columnName, String value,int expireTime ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumn(columnName, value,expireTime);
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, ByteBuffer value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value, null)
	        ;
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, Date value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value);
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, int value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value);
	    }
	    
	    public void generateNonCounter(String cfName,String key,String columnName, boolean value ,MutationBatch m) {
	        m.withRow(this.accessColumnFamily(cfName), key)
	        .putColumnIfNotNull(columnName, value);
	    }
	    public void  deleteAll(String cfName){
			try {
				getKeyspace().truncateColumnFamily(this.accessColumnFamily(cfName));
			} catch (Exception e) {
				 logger.info("Error while deleting rows in method :deleteAll {} ",e);
			} 
	    }
	    
	    public void  deleteRowKey(String cfName,String key){
	    	MutationBatch m = getKeyspace().prepareMutationBatch();
			try {
				m.withRow(this.accessColumnFamily(cfName), key)
				.delete()
				;
				
				m.execute();
			} catch (Exception e) {
				 logger.info("Error while deleting rows in method :deleteRowKey {} ",e);
			} 
	    }
	    
	    public void  deleteColumn(String cfName,String key,String columnName){
	    	MutationBatch m = getKeyspace().prepareMutationBatch();
			try {
				m.withRow(this.accessColumnFamily(cfName), key)
				.deleteColumn(columnName)
				;
				m.execute();
			} catch (Exception e) {
				 logger.info("Error while deleting rows in method :deleteColumn {} ",e);
			} 
	    }
	    public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

			ColumnFamily<String, String> aggregateColumnFamily;

			aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());

			return aggregateColumnFamily;
		}

	/*	public void saveResourceListData(String cfName, String key, List<Map<String, Object>> resourceList, String keySpaceType) {
			Integer partitionSize = 3000;
			Keyspace keySpace = getKeyspace();
			if (keySpaceType != null && keySpaceType.equalsIgnoreCase("AWS")) {
				keySpace = getAwsKeyspace();
			}
			try {
				Map<String, String> tableDatatype = schdulerService.getTablesDataTypeCache().get(cfName);
				for (List<Map<String, Object>> resourceSubList : Lists.partition(resourceList, partitionSize)) {
					MutationBatch m = keySpace.prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL);
					for (Map<String, Object> resource : resourceSubList) {
						if (resource != null && !resource.isEmpty() && resource.containsKey(key) && StringUtils.trimToNull(resource.get(key).toString()) != null) {
							ColumnListMutation<String> value = m.withRow(this.accessColumnFamily(cfName), resource.get(key).toString());
							for (Map.Entry<String, Object> entry : resource.entrySet()) {
								// System.out.println("Entries >>>== "+entry.getKey()+ " : " +entry.getValue());
								if (tableDatatype.containsKey(entry.getKey())) {
									if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("int") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("integer")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? Integer.parseInt(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("string") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("text")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("long") || tableDatatype.get(entry.getKey()).equalsIgnoreCase("bigint")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? Long.parseLong(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("boolean")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? Boolean.valueOf((entry.getValue().toString())) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("timestamp")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? dateFormatter.parse(entry.getValue().toString()) : null, null);
									} else if (tableDatatype.get(entry.getKey()).equalsIgnoreCase("date")) {
										value.putColumn(entry.getKey(), entry.getValue() != null ? (Date) entry.getValue() : null, null);
									}
								}
							}
						}
					}
					m.execute();
				}
			} catch (Exception e) {
				logger.error("Error while save in method : saveBulkListData {}" + e);
			}  
		}
*/
		
		 public ColumnList<String> readWithKey(Keyspace keyspace, String cfName,String key){
		        
		    	ColumnList<String> result = null;
		    	try {
		              result = keyspace.prepareQuery(this.accessColumnFamily(cfName))
		                    .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		                    .getKey(key)
		                    .execute()
		                    .getResult()
		                    ;

		        } catch (Exception e) {
		            logger.error("Error while fetching data from method : readWithKey {} ", e);
		        }
		    	
		    	return result;
		    }
	}

	