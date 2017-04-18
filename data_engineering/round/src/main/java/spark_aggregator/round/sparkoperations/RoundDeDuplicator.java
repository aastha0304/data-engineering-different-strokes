package spark_aggregator.round.sparkoperations;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

import spark_aggregator.round.RoundMeta;

public class RoundDeDuplicator implements Function2<RoundMeta, RoundMeta, RoundMeta>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public RoundMeta call(RoundMeta v1, RoundMeta v2) throws Exception {
		// TODO Auto-generated method stub
		return v1;
	}

}