package chapter08.gis;

import java.util.ArrayList;

public class Polygon {
	public ArrayList<Point> points;
	public double xmin;
	public double xmax;
	public double ymin;
	public double ymax;
	public int count;
	public double distance_min = 10 / 111.2 * 1000;

	// On the Earth, 1 Degree =111.2 km
	// Distance between two points 10m, shoule be 10/111.2*1000 =0.008993
	// Degree;

	public Polygon(ArrayList<Point> points) {
		this.points = points;
		this.count = points.size();

		this.xmin = Double.MAX_VALUE;
		this.xmax = Double.MIN_VALUE;
		this.ymin = Double.MAX_VALUE;
		this.ymax = Double.MIN_VALUE;
		for (Point p : points) {
			if (p.x > this.xmax)
				this.xmax = p.x;
			if (p.x < this.xmin)
				this.xmin = p.x;
			if (p.y > this.ymax)
				this.ymax = p.y;
			if (p.y < this.ymin)
				this.ymin = p.y;
		}
	}

	public Boolean contains(Point p) {

		if (p.x >= xmax || p.x < xmin || p.y >= ymax || p.y < ymin)
			return false;

		int cn = 0;
		int n = points.size();
		for (int i = 0; i < n - 1; i++) {
			if (points.get(i).y != points.get(i + 1).y
					&& !((p.y < points.get(i).y) && (p.y < points.get(i + 1).y))
					&& !((p.y > points.get(i).y) && (p.y > points.get(i + 1).y))) {// rule#3:
																					// erase
																					// the
																					// condition
																					// of
																					// horizonal
																					// line
				double uy = 0;
				double by = 0;
				double ux = 0;
				double bx = 0;
				int dir = 0;
				if (points.get(i).y > points.get(i + 1).y) {
					uy = points.get(i).y;
					by = points.get(i + 1).y;
					ux = points.get(i).x;
					bx = points.get(i + 1).x;
					dir = 0;// downward
				} else {
					uy = points.get(i + 1).y;
					by = points.get(i).y;
					ux = points.get(i + 1).x;
					bx = points.get(i).x;
					dir = 1;// upward
				}

				double tx = 0;
				if (ux != bx) {
					double k = (uy - by) / (ux - bx);
					double b = ((uy - k * ux) + (by - k * bx)) / 2;
					tx = (p.y - b) / k;
				} else
					tx = ux;

				if (tx > p.x) {// rule#4: the insect point should locate the
								// right side of p
					if (dir == 1 && p.y != points.get(i + 1).y)// rule#1: upward
																// do not count
																// the last
																// point
						cn++;
					else if (p.y != points.get(i).y)// rule#2: downward do not
													// count the first point
						cn++;
				}
			}
		}
		if (cn % 2 == 0)
			return false;
		else
			return true;
	}

	public boolean matchToRoad(Point p, int roadWidth) {
		// TODO Auto-generated method stub

		int n = points.size();
		for (int i = 0; i < n - 1; i++) {
			double distance = Math.sqrt(Math.pow(points.get(i).y - p.y, 2)
					+ Math.pow(points.get(i).x - p.x, 2));
			if (distance < roadWidth / 2.0 * Math.sqrt(2.0)) // sqrt(2) * width
				return true;

		}

		return false;
	}

	public boolean matchToRoad(Point p, int roadWidth, ArrayList<Point> ps) {
		// TODO Auto-generated method stub
		double minD = Double.MAX_VALUE;
		int n = ps.size();
		for (int i = 0; i < n - 2; i++) {
			double distance = Polygon.pointToLine(ps.get(i).x, ps.get(i).y,
					ps.get(i + 1).x, ps.get(i + 1).y, p.x, p.y) * 111.2 * 1000;
			if (distance < minD)
				minD = distance;
			System.out.println("distance=" + distance);
			if (distance < roadWidth / 2.0 * Math.sqrt(2.0)) // sqrt(2) * width
				return true;
		}

		return false;
	}

	public static double pointToLine(double x1, double y1, double x2,
			double y2, double x0, double y0) {
		double space = 0;
		double a, b, c;
		a = Polygon.lineSpace(x1, y1, x2, y2);// 线段的长度
		b = lineSpace(x1, y1, x0, y0);// (x1,y1)到点的距离
		c = lineSpace(x2, y2, x0, y0);// (x2,y2)到点的距离
		if (c <= 0.000001 || b <= 0.000001) {
			space = 0;
			return space;
		}
		if (a <= 0.000001) {
			space = b;
			return space;
		}
		if (c * c >= a * a + b * b) {
			space = b;
			return space;
		}
		if (b * b >= a * a + c * c) {
			space = c;
			return space;
		}
		double p = (a + b + c) / 2;// 半周长
		double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));// 海伦公式求面积
		space = 2 * s / a;// 返回点到线的距离（利用三角形面积公式求高）
		return space;
	}

	// 计算两点之间的距离
	public static double lineSpace(double x1, double y1, double x2, double y2) {
		double lineLength = 0;
		lineLength = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
		return lineLength;
	}

	public static double DistancePointToLine(double x1, double y1, double x2,
			double y2, double x, double y) {// 这是点到线的距离

		if (y1 == y2)// 线段为平行于X轴
		{
			if (Math.min(x1, x2) < x && Math.max(x1, x2) > x)// 垂足在线段内
			{
				return Math.abs(ComputeD(y1, x, y, x));// 点与该线的距离为TempP与P的的距离
			} else {
				return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));// 返回到某个端点的距离
			}
		}
		if (x1 == x2)// 线段为平行于Y轴
		{
			if (Math.min(y1, y2) < y && Math.max(y1, y2) > y)// 垂足在线段内
			{
				return ComputeD(y, x1, y, x);// 点与该线的距离为TempP与P的的距离
			} else {
				return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));// 返回到某个端点的距离
			}
		} else// 该线段不平行于X轴也不平行于Y轴
		{
			double k = (y2 - y1) / (x2 - x1); // 线段的斜率
			double TempX, TempY;
			TempX = (Math.pow(k, 2.0) * x1 + k * (y - y1) + x)
					/ (Math.pow(k, 2.0) + 1.0);
			TempY = k * (TempX - x1) + y1;
			if (TempX < -180 || TempX > 180 || TempY < -90 || TempY > 90) {
				return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));// 返回到某个端点的距离
			}
			double TempDis1 = (ComputeD(TempY, TempX, y1, x1) + ComputeD(TempY,
					TempX, y2, x2));
			double TempDis2 = ComputeD(y1, x1, y2, x2);
			if ((TempDis1 - TempDis2) < 0.001) // 垂足在线内
			{
				return (ComputeD(TempY, TempX, y, x));// 点与该线的距离为TempP与P的的距离
			} else {
				return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));// 返回到某个端点的距离
			}
		}
	}

	public static double ComputeD(double lat_a, double lng_a, double lat_b,
			double lng_b) {// 两点经纬度距离算法
		int EARTH_RADIUS = 6378137;

		double radLat1 = (lat_a * Math.PI / 180.0);
		double radLat2 = (lat_b * Math.PI / 180.0);
		double a = radLat1 - radLat2;
		double b = (lng_a - lng_b) * Math.PI / 180.0;
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(radLat1) * Math.cos(radLat2)
				* Math.pow(Math.sin(b / 2), 2)));
		s = s * EARTH_RADIUS;

		return s;
	}

}
