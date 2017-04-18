package spark_aggregator.league.utils;

import java.util.Comparator;

public class FeeSizeComparator implements Comparator<FeeSizeRanges>{

	@Override
	public int compare(FeeSizeRanges o1, FeeSizeRanges o2) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		if(o1.getFee()>o2.getFee())
			return 1;
		else if(o1.getFee()<o2.getFee())
			return -1;
		else if(o1.getSize()>o2.getSize())
			return 1;
		else if(o1.getSize()<o2.getSize())
			return -1;
		else
			return 0;
	}
}