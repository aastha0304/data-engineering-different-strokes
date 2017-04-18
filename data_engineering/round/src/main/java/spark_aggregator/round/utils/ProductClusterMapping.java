package spark_aggregator.round.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class ProductClusterMapping {
	private static volatile Broadcast<HashMap<Integer, Integer>> instance	= null;
//	private ProductClusterMapping(String path){
//	}
	
	public static Broadcast<HashMap<Integer, Integer>> getInstance(JavaSparkContext jsc, String path) {
		if (instance == null) {
		      synchronized (ProductClusterMapping.class) {
		        if (instance == null) {
		        	HashMap<Integer, Integer> p2c = new HashMap<>();
		        	
		    		try(BufferedReader br = new BufferedReader(new FileReader(path))) {
		    			int lineNo = 1;
		    		    for(String line; (line = br.readLine()) != null; ) {
		    		        String[] kvals = line.split(",");
		    		        if(kvals.length == 2){
		    		        	if(lineNo!=1)
		    		        		p2c.put(Integer.parseInt(kvals[0].trim()),Integer.parseInt(kvals[1].trim()));
		    		        	lineNo++;
		    		        }
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