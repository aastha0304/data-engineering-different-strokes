package spark_aggregator.round;

public class RoundMeta {
	long id;
	long clusterId;
	long type;
	long country;
	long game;
	long tourFormat;
	long tourMatch;
	long openTime;
	long roundLockTime;
	boolean isActive;
	long halfTime;
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getClusterId() {
		return clusterId;
	}
	public void setClusterId(long clusterId) {
		this.clusterId = clusterId;
	}
	public long getType() {
		return type;
	}
	public void setType(long type) {
		this.type = type;
	}
	public long getCountry() {
		return country;
	}
	public void setCountry(long country) {
		this.country = country;
	}
	public long getGame() {
		return game;
	}
	public void setGame(long game) {
		this.game = game;
	}
	public long getTourFormat() {
		return tourFormat;
	}
	public void setTourFormat(long tourFormat) {
		this.tourFormat = tourFormat;
	}
	public long getTourMatch() {
		return tourMatch;
	}
	public void setTourMatch(long tourMatch) {
		this.tourMatch = tourMatch;
	}
	public long getOpenTime() {
		return openTime;
	}
	public void setOpenTime(long openTime) {
		this.openTime = openTime;
	}
	public long getRoundLockTime() {
		return roundLockTime;
	}
	public void setRoundLockTime(long roundLockTime) {
		this.roundLockTime = roundLockTime;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	public long getHalfTime() {
		return halfTime;
	}
	public void setHalfTime(long halfTime) {
		this.halfTime = halfTime;
	}
}
