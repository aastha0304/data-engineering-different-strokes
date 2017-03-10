package spark_aggregator.league;

import java.io.Serializable;
import java.util.Set;

public class UsableColumns implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	long users;
	double amount;
	
	Set<Long> fullLeagues;
	
	
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public long getUsers() {
		return users;
	}
	public void setUsers(long users) {
		this.users = users;
	}
	public Set<Long> getFullLeagues() {
		return fullLeagues;
	}
	public void setFullLeagues(Set<Long> fullLeagues) {
		this.fullLeagues = fullLeagues;
	}
	
}
