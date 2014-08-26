package org.gooru.insights.constants;

public interface APIConstants {

	public enum hasdata{
		HAS_FEILDS("hasFields"),HAS_DATASOURCE("hasDataSource"),HAS_GROUPBY("hasGroupBy"),HAS_INTERVALS("hasIntervals"),
		HAS_FILTER("hasFilter"),HAS_AGGREGATE("hasAggregate"),HAS_LIMIT("hasLimit"),HAS_Offset("hasOffset"),HAS_SORTBY("hasSortBy"),HAS_SORTORDER("hasSortOrder");
		
		private String name;
		
		public String check(){
		return name;	
		}
		
		private hasdata(String name){
			this.name = name;
		}
	}
}
