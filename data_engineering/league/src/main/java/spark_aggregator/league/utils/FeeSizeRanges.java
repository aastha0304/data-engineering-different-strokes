package spark_aggregator.league.utils;

import java.io.Serializable;

public class FeeSizeRanges implements Comparable<FeeSizeRanges>, Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	float fee;
	int size;
	public float getFee() {
		return fee;
	}
	public void setFee(float fee) {
		this.fee = fee;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	@Override
	public int compareTo(FeeSizeRanges obj2) {
		// TODO Auto-generated method stub
		if(this.getFee()>obj2.getFee())
			return 1;
		else if(this.getFee()<obj2.getFee())
			return -1;
		else if(this.getSize()>obj2.getSize())
			return 1;
		else if(this.getSize()<obj2.getSize())
			return -1;
		else
			return 0;
	}
}

