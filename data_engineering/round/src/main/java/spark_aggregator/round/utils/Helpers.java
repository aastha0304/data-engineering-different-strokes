package spark_aggregator.round.utils;

import java.util.Date;

import com.couchbase.client.java.document.json.JsonObject;

public class Helpers {
	public static long calcHalfTime(long start, long end){
		return start + ((end-start)/2);
	}

	public static long getDays(long l, int i) {
		return l-(i*24*3600*1000l);
	}
	
	public static double f(JsonObject rowObject) {
		long current = System.currentTimeMillis();
		double timeInHours = inHours(current, st(rowObject));
		return Math.pow(0.996, (timeInHours));
    }
	public static long st(JsonObject rowObject) {
        return rowObject.getLong(Constants.OPENTIME_KEY);
    }
	private static double inHours(long end, long start){
		long secs = (end - start) / 1000;
		double hours = secs / 3600; 
		return hours;
	}

	public static double div(double num, long den) {
		// TODO Auto-generated method stub
		if(den==0)
			return 0;
		else
			return num/den;
	}
}
