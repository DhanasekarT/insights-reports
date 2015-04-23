package org.gooru.insights.constants;

public final class CassandraConstants {


	public enum CassandraConfigs{
		SEEDS("cassandra.hosts"),PORT("cassandra.port"),CLUSTER("cassandra.clusterName"),INSIGHTS_KEYSPACE("cassandra.keyspace.insights"),DATA_CENTRE("dataCentre");
		
		private String property;
		
		private CassandraConfigs(String name){
		this.property = name;	
		}
		
		public String cassandraConfig(){
			return property;
		}
	}
	
	public enum Keyspaces{
		INSIGHTS("insights"),SEARCH("search");
		
		private String keyspace;
		
		private Keyspaces(String name){
		this.keyspace = name;	
		}
		
		public String keyspace(){
			return keyspace;
		}
	}
	
	public enum CassandraRowKeys{
		FILED_ARRAY_HANDLER("fieldArrayHandler");
		
		private String index;
		
		private CassandraRowKeys(String name){
			this.index = name;
		}

		public String CassandraRowKey(){
			return index;
		}
	}
	
	public enum ColumnFamilies{
		EVENT_FIELDS("event_fields"), CONFIG_SETTINGS("config_settings"), JOB_CONFIG_SETTINGS("job_config_settings"), 
		QUERY_REPORTS("query_reports");
		
		private String columnFamilyName;
		
		private ColumnFamilies(String name){
		this.columnFamilyName = name;	
		}
		
		public String columnFamily(){
			return columnFamilyName;
		}
	}
}
