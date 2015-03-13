package chapter08.gis;

public class GPSRcrd extends Point {
	public int vel;
	public int bearing;

	public GPSRcrd() {
	}

	public GPSRcrd(double x, double y, int vel, int bearing) {
		this.x = x;
		this.y = y;
		this.vel = vel;
		this.bearing = bearing;
	}

}
