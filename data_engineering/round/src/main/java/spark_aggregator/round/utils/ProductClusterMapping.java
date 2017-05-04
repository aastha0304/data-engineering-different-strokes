package spark_aggregator.round.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class ProductClusterMapping {
	private static Broadcast<HashMap<Long, Long>> instance	;
	private ProductClusterMapping(String path){
	}
	
	public static Broadcast<HashMap<Long, Long>> getInstance(JavaSparkContext jsc, String path) {
		if (instance == null) {
		      synchronized (ProductClusterMapping.class) {
		        if (instance == null) {
		        	HashMap<Long, Long> p2c = new HashMap<>();
		    		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
		    		    for(String line; (line = br.readLine()) != null; ) {
		    		        String[] kvals = line.split(",");
		    		        if(kvals.length == 2)
		    		        	p2c.put(Long.parseLong(kvals[0].trim()), Long.parseLong(kvals[1].trim())+100);
		    		    }
		    		    // line is not visible here.
		    		} catch (FileNotFoundException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    		} catch (IOException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    		}
		    		instance = jsc.broadcast(p2c);
		        }
		      }
		    }
		 return instance;
	}
}