package spark_aggregator.league.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class ProductClusterMapping {
	private static volatile Broadcast<List<FeeSizeRanges>> instance	= null;
//	private ProductClusterMapping(String path){
//	}
	
	public static Broadcast<List<FeeSizeRanges>> getInstance(JavaSparkContext jsc, String path) {
		if (instance == null) {
		      synchronized (ProductClusterMapping.class) {
		        if (instance == null) {
		        	List<FeeSizeRanges> p2c = new ArrayList<>();
		        	
		        	try(BufferedReader br = new BufferedReader(new FileReader(path))) {
		    			int lineNo = 1;
		    		    for(String line; (line = br.readLine()) != null; ) {
		    		        String[] kvals = line.split(",");
		    		        if(kvals.length == 5){
		    		        	if(lineNo!=1){
		    		        		FeeSizeRanges feeSizeRange = new FeeSizeRanges();
		    		        		if(!kvals[2].contains("99999"))
		    		        			feeSizeRange.setMaxFee(Float.parseFloat(kvals[2].trim()));
		    		        		else
		    		        			feeSizeRange.setMaxFee(999999999);
		    		        		if(!kvals[4].contains("99999"))
		    		        			feeSizeRange.setMaxSize(Integer.parseInt(kvals[4].trim()));
		    		        		else
		    		        			feeSizeRange.setMaxSize(999999999);
		    		        		feeSizeRange.setMinFee(Float.parseFloat(kvals[1].trim()));
		    		        		feeSizeRange.setMinSize(Integer.parseInt(kvals[3].trim()));
		    		        		p2c.add(feeSizeRange);
		    		        	}
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
		//display(instance.value());
		return instance;
	}

	private static void display(List<FeeSizeRanges> p2c) {
		// TODO Auto-generated method stub
			for(FeeSizeRanges f: p2c ){
				System.out.println("fee size value : "+f.minFee+" "+f.maxFee+" "+f.minSize+" "+f.maxSize+" ");
			
			}
	}
}