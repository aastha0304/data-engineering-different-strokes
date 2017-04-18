package spark_aggregator.round.utils;

public class Constants {
	public static final String ROUNDTOPIC = "agg0-round_lge";
	public static final String VALUESCHEMAPREFIX = "-value";
	public static final String KEYSCHEMAPREFIX = "-key";
	public static final String OPENTIME_KEY = "openAt";
	public static final String PRODUCTID_KEY = "ProductId";
	public static final String CLUSTERID_KEY = "ClusterId";
	public static final String ROUNDID_KEY = "RoundId";
	public static final String FROMOFFSET = "from_offset";
	public static final String UNTILOFFSET = "until_offset";
	public static final String PARTITION_KEY = "partitions";
	public static final String META_HEADER = "meta";
	public static final String ROUNDLOCKTIME_KEY = "RoundStartTime";
	public static final String HALFTIME_KEY = "halftime";
	public static final String ALLROUNDS_KEY = "all_rounds";
	public static final long INDIA_KEY = 1;
	public static final long CRICKET_KEY = 1;
	public static final String TOURFORMAT_KEY = "TourFormat";
	public static final String TOURMATCH_KEY = "TourMatch";
	public static final String TIMEBASEDAGG_KEY = "timevalueMetrics";
	public static final String SUM = "sum";
	public static final String COUNT = "count";
	public static final String OTHERCOUNT = "o_count";
	public static final String OTHERSUM = "o_sum";
	public static final String HALFTIMENESTED_KEY = "timestamp";
}
