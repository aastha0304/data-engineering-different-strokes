package spark_aggregator.league.utils;

import java.util.Set;
import java.util.HashMap;
import java.util.Map.Entry;

public class Helpers {
	public static void display(HashMap<Integer, Integer>p2c){
		Set<Entry<Integer, Integer>> x = p2c.entrySet();
		System.out.println("size of map "+p2c.size());
		for(Entry<Integer, Integer> y: x){
			System.out.println(y.getKey()+" "+y.getValue()+" "+y.getKey().getClass()+" "+(Integer)y.getKey());
		}
	}
	
}
