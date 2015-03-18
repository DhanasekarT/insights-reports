package org.gooru.insights.constants;

public class APIConstants {
	
	/**
	 * Symbols
	 */
	public final static String COMMA = ",";
	
	public final static String COLON = ":";
	
	public final static String DOT = ".";
	
	public final static String EMPTY= "";
	
	public final static String PIPE = "|";

	public final static String EMPTY_JSON_ARRAY = "[{}]";
	
	public final static String SEPARATOR = "~";
	
	public final static String WILD_CARD = "*";
	
	/**
	 * view Attributes
	 */
	public final static String VIEW_NAME = "VIEW_NAME";
	
	public final static String RESPONSE_NAME = "RESPONSE_NAME";
	
	/**
	 * Date formats
	 */
	public final static String DEFAULT_FORMAT = "yyyy-MM-dd kk:mm:ss";

	/**
	 * Environment constants
	 */
	public final static String DEV = "dev";
	
	public final static String PROD = "prod";
	
	/**
	 * cache constants
	 */
	public final static String CACHE_PREFIX = "insights";
	
	public final static String CACHE_PREFIX_ID = "key";
	
	public final static String GOORU_PREFIX = "authenticate_";
	
	/**
	 * Authorization constants
	 */
	public final static String AP_SELF_ACTIVITY = "AP_SELF_ACTIVITY";
	
	public final static String AP_SELF_PII = "AP_SELF_PII";
	
	public final static String AP_PARTY_PUBLIC = "AP_PARTY_PUBLIC";
	
	public final static String AP_PARTY_PII = "AP_PARTY_PII";
	
	public final static String AP_ALL_PARTY_ALL_DATA = "AP_ALL_PARTY_ALL_DATA";
	
	public final static String AP_PARTY_ALL_DATA = "AP_PARTY_ALL_DATA";
	
	public final static String AP_SYSTEM_PUBLIC = "AP_SYSTEM_PUBLIC";
	
	public final static String AP_OWN_CONTENT_USAGE = "AP_OWN_CONTENT_USAGE";
	
	public final static String AP_PARTY_OWN_CONTENT_USAGE = "AP_PARTY_OWN_CONTENT_USAGE";
	
	public final static String AP_PARTY_ACTIVITY = "AP_PARTY_ACTIVITY";
	
	public final static String AP_APP_SESSION_PARTY_ACTIVITY = "AP_APP_SESSION_PARTY_ACTIVITY";
	
	public final static String AP_APP_SESSION_PARTY_CONTENT_USAGE = "AP_APP_SESSION_PARTY_CONTENT_USAGE";
	
	public final static String AP_PARTY_ACTIVITY_RAW = "AP_PARTY_ACTIVITY_RAW";
	
	public final static String DI_REPORTS = "di_reports";
	
	public final static String ANONYMOUS_USERDATA_SOURCE = "anonymoususerdata";
	
	/**
	 * parameter constants
	 */
	
	public final static String GRANULARITY_NAME = "granularity";
	
	public final static String DATA_SOURCE = "dataSource";
	
	public final static String FIELDS = "fields";
	
	public final static String FILTER_FIELDS = "filterFields";
	
	public final static String GROUP_BY = "groupBy";
	
	public final static String SELECTOR = "selector";
	
	public final static String AGGREGATE_ATTRIBUTE = "aggregateAttribute";
	
	public final static String RANGE_ATTRIBUTE = "rangeAttribute";
	
	public final static String MULTIPLE_GROUPBY = "multipleGroupby";
	
	public final static String LOGICAL_OPERATOR = "logicalOperator";
	
	public final static String FILTERS = "filterAtributes";
	
	public final static String SORT_BY = "sortBy";

	public final static String DESC = "DESC";
	
	public final static String PERMISSIONS = "permissions";
	
	public final static String FIELD_NAME = "field_name";
	
	public final static String DEPENDENT_NAME = "dependent_name";
	
	public final static String USER_TOKEN = "userToken";
	
	public final static String PARTY_UID = "partyUid";
	
	public final static String USER_ROLE_SETSTRING = "userRoleSetString";
	
	public final static String USER_CREDENTIAL = "userCredential";
	
	public final static String USER = "user";
	
	public final static String PARTY_PERMISSIONS = "partyPermissions";

	public final static String FIRST_NAME = "firstName";
	
	public final static String LAST_NAME = "lastName";
	
	public final static String EMAIL_ID = "emailId";
	
	public final static String GRANULARITY[] = {"YEAR","MONTH","QUATOR","WEEK","DAY","D","W","M","Y","year","month","quator","week","day"};
	
	public final static String QUERY = "query";
	
	public final static String QUERYS = "query's";
	
	public final static String CONTENTORGUID = "contentOrganizationUId";
	
	public final static String USERORGID = "userOrganizationUId";
	
	public final static String GOORUUID = "gooruUId";
	 
	public final static String USERUID = "userUid";
	
	public final static String CREATORUID = "creatorUid";
	
	public final static String CONTENT_ORG_UID = "content_organization_uid";
	
	public final static String USER_ORG_UID = "user_organization_uid";

	public final static String GOORU_UID = "gooru_uid";
	
	public final static String CREATOR_UID = "creator_uid";
	
	public final static String ACTIVITY = "rawData";
	
	public final static String CONTENT = "content";
	
	public final static String USER_DATA = "userdata";
	
	public final static String DEFAULTORGUID = "4261739e-ccae-11e1-adfb-5404a609bd14";

	public final static String ACTIVITYDATASOURCES =  ".*rawData.*|.*rawdata.*|.*activity.*|.*Activity.*";
	
	public final static String CONTENTDATASOURCES =  ".*content.*|.*resource.*";
	
	public final static String RESTRICTEDPERMISSION =  ".*AP_PARTY_ACTIVITY_RAW.*|.*AP_PARTY_PII.*|.*AP_ALL_PARTY_ALL_DATA.*|.*AP_PARTY_ALL_DATA.*";

	public final static String USERDATASOURCES =  ".*userData.*|.*userdata.*|.*user.*|.*User.*";
	
	public final static String USERFILTERPARAM =  ".*user_uid.*|.*userUid.*|.*gooru_uid.*|.*gooruUId.*|.*creatorUid.*|.*creator_uid.*";
	
	public final static String ORGFILTERPARAM =  ".*contentOrganizationUId.*|.*userOrganizationUId.*|.*content_organization_uid.*|.*user_organization_uid.*|.*organizationUId.*|.*contentOrganizationUid.*|.*userOrganizationUid.*";
	
	/**
	 * Serializer Excludes
	 */
	public final static String EXCLUDE_CLASSES = "*.class";
	
	/**
	 * Message constants
	 */
	public final static String TOMCAT_STATUS = "Tomcat started";
	
	public final static String STATUS_NAME = "STATUS_NAME";
	
	public final static String PARTY_PERMISSION_MESSAGE = "party permissions:";
	
	public final static String OLD_QUERY = "old query:";
	
	public final static String NEW_QUERY = "new query:";

	public final static String DELETED = "DELETED";
	
	public final static String UPDATE = "UPDATE";
	
	public final static String INSERTED = "INSERTED";

	public final static String NOT_FOUND = "NOT_FOUND";
	
	public final static String FAILED = "FAILED";
	
	public final static String STATUS = "STATUS";
	
	public final static String CACHE_CLEAR = "CACHE_CLEAR";
	
	public final static String DATA = "DATA";
	
	public final static String CONNECTION = "CONNECTION";
	
	public final static String ALLOWED_ORG = "allowedOrg";
	
	public final static String ROLES = "roles";
	
	public static final String QUERY_ID = "queryId";
	
	public static final String ASC = "ASC";

	/**
	 * Logical Enumaration 
	 */
	public static enum Hasdatas {
		
		HAS_FEILDS("hasFields"), HAS_DATASOURCE("hasDataSource"), HAS_GRANULARITY("hasGranularity"), 
		HAS_GROUPBY("hasGroupBy"), HAS_INTERVALS("hasIntervals"), HAS_FILTER("hasFilter"), 
		HAS_AGGREGATE("hasAggregate"), HAS_PAGINATION("hasPagination"), HAS_LIMIT("hasLimit"), 
		HAS_Offset("hasOffset"), HAS_SORTBY("hasSortBy"), HAS_SORTORDER("hasSortOrder"),HAS_RANGE("hasRange");

		private String name;

		public String check() {
			return name;
		}

		private Hasdatas(String name) {
			this.name = name;
		}
	}
	
	public static enum LogicalConstants {

		FIELD("field"),DATE("date");
		
		private String field;
		
		public String value() {
			return field;
		}
		
		private LogicalConstants(String name) {
			this.field = name;
		}
		
	}

	public static enum DateHistory { 
		
		D_CHECKER("(.*)D|(.*)d"), W_CHECKER("(.*)W|(.*)w"), H_CHECKER("(.*)H|(.*)h"),
		K_CHECKER("(.*)K|(.*)k"), S_CHECKER("(.*)S|(.*)s"), D_REPLACER("D|d"),
		W_REPLACER("W|w"), H_REPLACER("H|h"), K_REPLACER("K|k"),
		S_REPLACER("S|s");
		
		private String replacer;
		
		public String replace() {
			return replacer;
		}
		private DateHistory(String replacer) {
			this.replacer = replacer;
		}
	}
	
	public static enum DateFormats {

		DEFAULT("yyyy-MM-dd hh:kk:ss"), YEAR("yyyy"), QUARTER("yyyy-MM-dd"), MONTH("yyyy-MM"), 
		WEEK("yyyy-MM-dd"), DAY("yyyy-MM-dd"), HOUR("yyyy-MM-dd hh"), MINUTE("yyyy-MM-dd hh:kk"), 
		SECOND("yyyy-MM-dd hh:kk:ss"), D("yyyy-MM-dd"), W("yyyy-MM-dd"), 
		H("yyyy-MM-dd hh"), K("yyyy-MM-dd hh:kk"), S("yyyy-MM-dd hh:kk:ss"), MILLISECOND("yyyy-MM-dd hh:kk:ss.SSS"),  NANOSECOND("yyyy-MM-dd hh:kk:ss.SSS");

		private String format;

		public String format() {
			return format;
		}

		private DateFormats(String format) {
			this.format = format;
		}
	}
	
	public static enum FormulaFields {

		FORMULA("formula"), REQUEST_VALUES("requestValues"), METRICS("metrics"), 
		NAME("name"), AGGREGATIONS("aggregations"), FIELD("field"), 
		BUCKETS("buckets"), TOTAL_ROWS("totalRows"), KEY("key"), 
		DOC_COUNT("doc_count"), KEY_AS_STRING("key_as_string"), SOURCE("_source"), 
		HITS("hits"), TOTAL("total"), FIELDS("fields"),FROM("from"),TO("to"),FROM_AS_STRING("from_as_string"),TO_AS_STRING("to_as_string"), 
		FILTERS("filters");

		private String field;

		public String getField() {
			return field;
		}

		private FormulaFields(String field) {
			this.field = field;
		}
	}

	public static enum DataTypes {
	
		STRING("String"), LONG("Long"), INTEGER("Integer"), 
		DOUBLE("Double"), SHORT("Short"), DATE("Date");

		private String field;

		public String dataType() {
			return field;
		}

		private DataTypes(String field) {
			this.field = field;
		}
	}
	
	public static enum AggregateFields {

		SUM("sum"), AVG("avg"), MAX("max"),
		MIN("min"),COUNT("count"),DISTINCT("distinct");
		
		private String field; 
		
		public String getField() {
			return field;
		}
		
		private AggregateFields(String field) {
			this.field = field;
		}
	}
	
	public static enum EsFilterFields {
		
		AND("AND"), OR("OR"), NOT("NOT"),
		GT("gt"), FILTERS("filters"), SELECTOR("selector"),
		RG("rg"), NRG("nrg"), EQ("eq"),
		LK("lk"), EX("ex"), IN("in"),
		LE("le"), GE("ge"), LT("lt");

		private String field;
		
		public String field() {
			return field;
		}
		
		private EsFilterFields(String field) {
			this.field = field;
		}
	}
	
	public static enum Numbers {
		FOUR("4"), FIVE("5");
		
		private String number;
		
		public String getNumber() {
			return number;
		}
		
		private Numbers(String number) {
			this.number = number;
		}
	}
}
