package org.gooru.insights.constants;

public class ESConstants {

	public enum EsConfigs{
		ROWKEY("es~connection"),DEV_ROWKEY("es~connection~dev"),ES_INDICES("es~indices"),INDEX("index_name"),CLUSTER("cluster_name"),ES_CLUSTER("cluster.name"),NODE("node_type"),HOSTS("host_names"),NODE_CLIENT("node_type"),TRANSPORT_CLIENT("transportClient"),PORTNO("port_no");
		
		private String property;
		EsConfigs(String name){
		this.property = name;	
		}
		public String esConfig(){
			return property;
		}
	}
	
	public enum EsSources{
		SOURCE("_source"),FIELDS("fields");
		
		private String property;
		EsSources(String name){
		this.property = name;	
		}
		public String esSource(){
			return property;
		}
	}
	
}
