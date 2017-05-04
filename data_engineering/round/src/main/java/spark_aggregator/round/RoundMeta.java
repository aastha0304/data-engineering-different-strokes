package spark_aggregator.round;

public class RoundMeta {
	long id;
	//long type;
	long country;
	long game;
	int tourFormat;
	int tourMatch;
	long openTime;
	long RoundStartTime;
	boolean isActive;
	long halftime;
	public long getCountry() {
		return country;
	}
	public long getGame() {
		return game;
	}
	public long getHalfTime() {
		return halftime;
	}
	public long getId() {
		return id;
	}
	public long getOpenTime() {
		return openTime;
	}
	public long getRoundStartTime() {
		return RoundStartTime;
	}
	public int getTourFormat() {
		return tourFormat;
	}
	public int getTourMatch() {
		return tourMatch;
	}
	public boolean isActive() {
		return isActive;
	}
	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}
	public void setCountry(long country) {
		this.country = country;
	}
	public void setGame(long game) {
		this.game = game;
	}
	public void setHalfTime(long halfTime) {
		this.halftime = halfTime;
	}
	public void setId(long id) {
		this.id = id;
	}
	public void setOpenTime(long openTime) {
		this.openTime = openTime;
	}
	public void setRoundStartTime(long roundLockTime) {
		this.RoundStartTime = roundLockTime;
	}
	public void setTourFormat(int tourFormat) {
		this.tourFormat = tourFormat;
	}
	public void setTourMatch(int tourMatch) {
		this.tourMatch = tourMatch;
	}
}
