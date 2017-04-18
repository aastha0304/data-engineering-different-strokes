package spark_aggregator.round.utils;

public class Helpers {
	public static long calcHalfTime(long start, long end){
		return start + ((end-start)/2);
	}
}
