package org.gooru.insights.constants;

import java.text.SimpleDateFormat;


public interface APIConstants {
	
	public static String DEFAULT_FORMAT = "yyyy-MM-dd kk:mm:ss";
	
	public static String COMMA = ",";
	
	public static String DEV = "dev";
	
	public static String PROD = "prod";
	
	public static String EMPTY= "";
	
	public static String PIPE = "|";
	
	public static String EMPTY_JSON_ARRAY = "[{}]";
	
	public static String DESC = "DESC";
	
	public static String CACHE_PREFIX ="insights";
	
	public static String DI_REPORTS ="di_reports";
	
	public static String CACHE_PREFIX_ID ="key";
	
	public static String FIELD_NAME = "field_name";
	
	public static String DEPENDENT_NAME = "dependent_name";
	
	public static String GOORU_PREFIX = "authenticate_";
	
	public static String SEPARATOR="~";
	
	public static String WILD_CARD="*";
	
	public static String AP_SELF_ACTIVITY = "AP_SELF_ACTIVITY";
	
	public static String AP_SELF_PII = "AP_SELF_PII";
	
	public static String AP_PARTY_PUBLIC = "AP_PARTY_PUBLIC";
	
	public static String AP_PARTY_PII = "AP_PARTY_PII";
	
	public static String AP_ALL_PARTY_ALL_DATA = "AP_ALL_PARTY_ALL_DATA";
	
	public static String AP_PARTY_ALL_DATA = "AP_PARTY_ALL_DATA";
	
	public static String AP_SYSTEM_PUBLIC = "AP_SYSTEM_PUBLIC";
	
	public static String AP_OWN_CONTENT_USAGE = "AP_OWN_CONTENT_USAGE";
	
	public static String AP_PARTY_OWN_CONTENT_USAGE = "AP_PARTY_OWN_CONTENT_USAGE";
	
	public static String AP_PARTY_ACTIVITY = "AP_PARTY_ACTIVITY";
	
	public static String AP_APP_SESSION_PARTY_ACTIVITY = "AP_APP_SESSION_PARTY_ACTIVITY";
	
	public static String AP_APP_SESSION_PARTY_CONTENT_USAGE = "AP_APP_SESSION_PARTY_CONTENT_USAGE";
	
	public static String AP_PARTY_ACTIVITY_RAW = "AP_PARTY_ACTIVITY_RAW";
	
	public static String CONTENTORGUID = "contentOrganizationUId";
	
	public static String USERORGID = "userOrganizationUId";
	
	public static String GOORUUID = "gooruUId";
	
	public static String USERUID = "userUid";
	
	public static String CREATORUID = "creatorUid";
	
	public static String CONTENT_ORG_UID = "content_organization_uid";
	
	public static String USER_ORG_UID = "user_organization_uid";
	
	public static String GOORU_UID = "gooru_uid";
	
	public static String CREATOR_UID = "creator_uid";
	
	public static String ACTIVITY = "rawData";
	
	public static String CONTENT = "content";
	
	public static String USER = "userdata";
	
	public static String DEFAULTORGUID = "4261739e-ccae-11e1-adfb-5404a609bd14";

	public String ACTIVITYDATASOURCES =  ".*rawData.*|.*rawdata.*|.*activity.*|.*Activity.*";
	
	public String CONTENTDATASOURCES =  ".*content.*|.*resource.*";
	
	public String RESTRICTEDPERMISSION =  ".*AP_PARTY_ACTIVITY_RAW.*|.*AP_PARTY_PII.*|.*AP_ALL_PARTY_ALL_DATA.*|.*AP_PARTY_ALL_DATA.*";

	public String USERDATASOURCES =  ".*userData.*|.*userdata.*|.*user.*|.*User.*";
	
	public String USERFILTERPARAM =  ".*user_uid.*|.*userUid.*|.*gooru_uid.*|.*gooruUId.*|.*creatorUid.*|.*creator_uid.*";
	
	public String ORGFILTERPARAM =  ".*contentOrganizationUId.*|.*userOrganizationUId.*|.*content_organization_uid.*|.*user_organization_uid.*|.*organizationUId.*|.*contentOrganizationUid.*|.*userOrganizationUid.*";
	
	public String GRANULARITY[] = {"YEAR","MONTH","QUATOR","WEEK","DAY","D","W","M","Y","year","month","quator","week","day"};
	
	public String ANONYMOUSUSERDATASOURCE = "anonymoususerdata";
	
	public enum hasdata{
		HAS_FEILDS("hasFields"),HAS_DATASOURCE("hasDataSource"),HAS_GRANULARITY("hasGranularity"),HAS_GROUPBY("hasGroupBy"),HAS_INTERVALS("hasIntervals"),
		HAS_FILTER("hasFilter"),HAS_AGGREGATE("hasAggregate"),HAS_PAGINATION("hasPagination"),HAS_LIMIT("hasLimit"),HAS_Offset("hasOffset"),HAS_SORTBY("hasSortBy"),HAS_SORTORDER("hasSortOrder");
		
		private String name;
		
		public String check(){
		return name;	
		}
		
		private hasdata(String name){
			this.name = name;
		}
	}
	
	public enum logicalConstants{

		FIELD("field"),DATE("date");
		
		private String field;
		public String value(){
			return field;
		}
		private logicalConstants(String name){
			this.field = name;
		}
		
	}

	public enum dateHistory{
		
		D_CHECKER("(.*)D|(.*)d"),W_CHECKER("(.*)W|(.*)w"),H_CHECKER("(.*)H|(.*)h"),K_CHECKER("(.*)K|(.*)k"),S_CHECKER("(.*)S|(.*)s"),
		D_REPLACER("D|d"),W_REPLACER("W|w"),H_REPLACER("H|h"),K_REPLACER("K|k"),S_REPLACER("S|s");
		
		private String replacer;
		public String replace(){
			return replacer;
		}
		private dateHistory(String replacer){
			this.replacer = replacer;
		}
	}
	
	public enum dateFormats{
	
		DEFAULT("yyyy-MM-dd hh:kk:ss"),YEAR("yyyy"),QUARTER("yyyy-MM-dd"),MONTH("yyyy-MM"),WEEK("yyyy-MM-dd"),DAY("yyyy-MM-dd"),HOUR("yyyy-MM-dd hh"),MINUTE("yyyy-MM-dd hh:kk"),SECOND("yyyy-MM-dd hh:kk:ss"),
		D("yyyy-MM-dd"),W("yyyy-MM-dd"),H("yyyy-MM-dd hh"),K("yyyy-MM-dd hh:kk"),S("yyyy-MM-dd hh:kk:ss");
		
		private String format;
		public String format(){
			return format;
		}
		private dateFormats(String format){
			this.format = format;
		}
	}
	
	public enum formulaFields{
		
		FORMULA("formula"),REQUEST_VALUES("requestValues"),METRICS("metrics"),NAME("name"),
		AGGREGATIONS("aggregations"),FIELD("field"),BUCKETS("buckets"),TOTAL_ROWS("totalRows"),
		KEY("key"),DOC_COUNT("doc_count"),KEY_AS_STRING("key_as_string"),SOURCE("_source"),HITS("hits"),
		TOTAL("total"),FIELDS("fields"),FILTERS("filters");

		private String field;
		public String field(){
			return field;
		}
		private formulaFields(String field){
			this.field = field;
		}
		}

	public enum dataTypes{
		STRING("String"),LONG("Long"),INTEGER("Integer"),DOUBLE("Double"),SHORT("Short"),DATE("Date");
		private String field;
		public String dataType(){
			return field;
		}
		private dataTypes(String field){
			this.field = field;
		}
	}
	
	public enum aggregateFields{

		SUM("sum"),AVG("avg"),MAX("max"),MIN("min"),COUNT("count"),DISTINCT("distinct");
		
		private String field;
		public String field(){
			return field;
		}
		private aggregateFields(String field){
			this.field = field;
		}
	}
	
	public enum esFilterFields {

		AND("AND"), OR("OR"), NOT("NOT"), GT("gt"), FILTERS("filters"), SELECTOR("selector"), RG("rg"), NRG("nrg"), EQ("eq"), LK("lk"), EX("ex"), IN("in"), LE("le"), GE("ge"), LT("lt");
		private String field;

		public String field() {
			return field;
		}

		private esFilterFields(String field) {
			this.field = field;
		}
	}
	
	enum ResourceType {

		PRESENTATION("ppt/pptx"), 
		VIDEO("video/youtube"), 
		QUESTION("question"), 
		ANIMATION_SWF("animation/swf"), 
		ANIMATION_KMZ("animation/kmz"), 
		IMAGE("image/png"), 
		RESOURCE("resource/url"), 
		HANDOUTS("handouts"), 
		CLASSPLAN("gooru/classplan"), 
		TEXTBOOK("textbook/scribd"), 
		STUDYSHELF("gooru/studyshelf"), 
		EXAM("exam/pdf"), 
		CLASSBOOK("gooru/classbook"), 
		NOTEBOOK("gooru/notebook"), 
		QB_QUESTION("qb/question"), 
		QB_RESPONSE("qb/response"), 
		ASSESSMENT_QUIZ("assessment-quiz"), 
		ASSESSMENT_EXAM("assessment-exam"), 
		ASSESSMENT_QUESTION("assessment-question"), 
		AM_ASSESSMENT_QUESTION("am:assessment-question"), 
		SCOLLECTION("scollection"), 
		SHELF("shelf"), 
		FOLDER("folder"), 
		ASSESSMENT("assessment"), 
		ASSIGNMENT("assignment"), 
		CLASSPAGE("classpage"), 
		PATHWAY("pathway"), 
		ALL("all"), 
		QUIZ("quiz"), 
		DOCUMENTS("documents"), 
		AUDIO("audio"), 
		READINGS("readings"), 
		MAPS("maps"), 
		CASES("cases"), 
		APPLICATION("application"), 
		OAUTH("oauth"), 
		LTI("lti"), 
		VIMEO_VIDEO("vimeo/video");

		private String type;

		ResourceType(String type) {
			this.type = type;
		}

		public String getType() {
			return this.type;
		}
	}
	
	String RESOURCE_TYPES = ResourceType.PRESENTATION.getType()+"|"+ResourceType.AUDIO.getType()+"|"+ResourceType.IMAGE.getType()+"|"+ResourceType.VIDEO.getType()+"|"+ResourceType.RESOURCE.getType()+"|"+ResourceType.ANIMATION_KMZ.getType()+"|"+ResourceType.ANIMATION_SWF.getType()+"|"+ResourceType.TEXTBOOK.getType()+"|"+ResourceType.VIMEO_VIDEO.getType()+"|"+ResourceType.HANDOUTS.getType()+"|"+ResourceType.EXAM.getType();
	
	String QUESTION_TYPES = ResourceType.ASSESSMENT_QUESTION.getType()+"|"+ResourceType.QB_QUESTION.getType()+"|"+ResourceType.QUESTION.getType();
	
	String COLLECTION_TYPES = ResourceType.SCOLLECTION.getType();
	
	String CLASSPAGE = ResourceType.CLASSPAGE.getType();
	
	String LIBRARY = "library";
	
	String AGENT = "Agent";
	
	SimpleDateFormat MINUTE_DATE_FORMATTER = new SimpleDateFormat("yyyyMMddkkmm");
	
	String XAPI_SUPPORTED_EVENTS = "item.create|item.edit|item.delete|profile.action|item.flag|item.rate|reaction.create|review.create|comment.create|resource.play|collection.play|collection.resource.play|library.view|item.load|classpage.view|user.login|user.logout";
	
	String PAGINATION_PARAMS= "offset|limit|sortOrder|startDate|endDate|sessionToken|email";
	
	String START_DATE = "startDate";
	
	String END_DATE = "endDate";
}
