package spark_aggregator.league.utils;

import java.io.Serializable;

public class FeeSizeRanges implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		float minFee;
		public float getMinFee() {
			return minFee;
		}
		public void setMinFee(float minFee) {
			this.minFee = minFee;
		}
		public int getMinSize() {
			return minSize;
		}
		public void setMinSize(int minSize) {
			this.minSize = minSize;
		}
		public float getMaxFee() {
			return maxFee;
		}
		public void setMaxFee(float maxFee) {
			this.maxFee = maxFee;
		}
		public int getMaxSize() {
			return maxSize;
		}
		public void setMaxSize(int maxSize) {
			this.maxSize = maxSize;
		}
		int minSize;
		float maxFee;
		int maxSize;
		
}




