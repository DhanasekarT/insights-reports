package org.ednovo.gooru.cassandra;

	import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.netflix.astyanax.Keyspace;
	import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Rows;

	public interface BaseCassandraRepo {

		public Column<String> readWithKeyColumn(String cfName,String key,String columnName);

		public ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList);

		public Rows<String, String> readWithKeyListColumnList(String cfName,Collection<String> keys,Collection<String> columnList);

		public ColumnList<String> readWithKey(String cfName,String key);

		public Rows<String, String> readWithKeyList(String cfName,Collection<String> key);

		public Rows<String, String> readCommaKeyList(String cfName,String... key);

		public Rows<String, String> readIterableKeyList(String cfName,Iterable<String> keys);

		public Rows<String, String> readAllRows(String cfName);

		public void saveBulkStringList(String cfName, String key,Map<String,String> columnValueList);

		public void saveBulkLongList(String cfName, String key,Map<String,Long> columnValueList);

		public void saveStringValue(String cfName, String key,String columnName,String value);

		public void saveIntegerValue(String cfName, String key,String columnName,Integer value);

		public void generateCounter(String cfName,String key,String columnName, long value ,MutationBatch m);

		public void generateNonCounter(String cfName,String key,String columnName, String value ,MutationBatch m);

		public void generateNonCounter(String cfName,String key,String columnName, long value ,MutationBatch m);
		
		public void saveListOfResource(String cfName, String key,List<Map<String,String>> resourceList);
		
		public void saveListOfResourceObject(String cfName, String key,List<Map<String,Object>> resourceList);
		
		public void saveLongValue(String cfName, String key,String columnName,long value);

		/*void saveListData(String cfName, String key,List<Map<String,Object>> resourceList, String keySpaceType);
		
		void saveListData(String cfName, String key,List<Map<String,Object>> resourceList);*/
		
		public Rows<String, String> readIndexedColumnList(String columnFamily,Map<String, String> whereColumn);

//		public void saveResourceListData(String cfName, String key, List<Map<String, Object>> resourceList, String keySpaceType) ;
		
		Rows<String, String> readIndexedColumn(String cfName,String columnName,String value);
		
		Rows<String, String> readIndexedColumn(String cfName,String columnName,long value);
		
		Rows<String, String> readIndexedColumn(String cfName,String columnName,int value);
		
		void saveMultipleStringValue(String cfName, String key,Map<String,Object> columns, String keySpaceType);
		
		Column<String> readWithKeyColumn(String cfName,String key,String columnName, String keySpaceType);
		
		ColumnList<String> readWithKeyColumnList(String cfName,String key,Collection<String> columnList, String keySpaceType);

		public void generateNonCounter(String cfName,String key,String columnName, ByteBuffer value ,MutationBatch m);
		
	    public void generateNonCounter(String cfName,String key,String columnName, Date value ,MutationBatch m);
	    
	    public void generateNonCounter(String cfName,String key,String columnName, int value ,MutationBatch m);
	    
	    public void generateNonCounter(String cfName,String key,String columnName, boolean value ,MutationBatch m);
		
		//public ColumnList<String> getResourceData(String cfName,String key);
	    
		public ColumnList<String> readWithKey(Keyspace keyspace, String cfName,String key);

	}
