package org.gooru.insights.constants;

public interface CassandraConstants {

	public enum cassandraConfigs{
		SEEDS("cassandra.hosts"),PORT("cassandra.port"),CLUSTER("cassandra.clusterName"),INSIGHTS_KEYSPACE("cassandra.keyspace.insights"),SEARCH_KEYSPACE("cassandra.keyspace.search");
		
		private String property;
		
		private cassandraConfigs(String name){
		this.property = name;	
		}
		
		public String cassandraConfig(){
			return property;
		}
	}
	
	public enum keyspaces{
		INSIGHTS("insights"),SEARCH("search");
		
		private String keyspace;
		
		private keyspaces(String name){
		this.keyspace = name;	
		}
		
		public String keyspace(){
			return keyspace;
		}
	}
	
	public enum cassRowKeys{
		RAW_KEY("");
		
		private String index;
		cassRowKeys(String name){
		this.index = name;
		}
		public String cassRowKey(){
			return index;
		}
	}
	
	public enum columnFamilies{
		CONNECTION_CONFIG_SETTING("connection_config_setting"),EVENT_FIELDS("event_fields");
		
		private String columnFamilyName;
		
		private columnFamilies(String name){
		this.columnFamilyName = name;	
		}
		
		public String columnFamily(){
			return columnFamilyName;
		}
	}
}
