package org.gooru.insights.constants;

public interface CassandraConstants {

	public enum cassandraConfigs{
		SEEDS("CASSANDRA_HOSTS"),PORT("CASSANDRA_PORT"),CLUSTER("CASSANDRA_CLUSTER_NAME"),INSIGHTS_KEYSPACE("CASSANDRA_INSIGHTS_KEYSPACE"),SEARCH_KEYSPACE("CASSANDRA_SEARCH_KEYSPACE");
		
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
	public enum columnFamilies{
		CONNECTION_CONFIG_SETTING("connection_config_setting");
		
		private String columnFamilyName;
		
		private columnFamilies(String name){
		this.columnFamilyName = name;	
		}
		
		public String columnFamily(){
			return columnFamilyName;
		}
	}
}
