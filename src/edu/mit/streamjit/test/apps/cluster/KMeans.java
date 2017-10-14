package edu.mit.streamjit.test.apps.cluster;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.util.Pair;

public class KMeans {
	
	static final int numbers=100;
	static List<Pair<Point,Integer>> p = new ArrayList<>();
	static List<Centroid> c = new ArrayList<>();
	static final int iteration = 10;

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		c.add(new Centroid(1, -31.85, -44.77));
		c.add(new Centroid(2, 35.16, 17.46));
		c.add(new Centroid(3, -5.16, 21.93));
		c.add(new Centroid(4, -24.06, 6.81));
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/KM.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new KM();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
//		System.out.println(bt);
//		System.out.println(et);
	}

	public static class KM extends Filter<String, String> {

		public KM() {
			super(Rate.create(1), Rate.create(0,100),Rate.create(0));
		}

		@Override
		public void work() {
			String edge = pop();
			if(edge.equals("END")){
				for(int i=0;i<iteration;i++){
					for(int j=0;j<p.size();j++){
						double minDistance = Double.MAX_VALUE;
						int closestCentroidId = -1;
						Point t = p.get(j).first;
						// check all cluster centers
						for (Centroid centroid : c) {
							// compute distance
							double distance = t.euclideanDistance(centroid);

							// update nearest cluster if necessary
							if (distance < minDistance) {
								minDistance = distance;
								closestCentroidId = centroid.id;
							}
						}
						p.set(j, Pair.make(t, closestCentroidId));
					}
					for(int j=0;j<c.size();j++){
						double X=0,Y=0;
						int n=0;
						Centroid centroid = c.get(j);
						for(Pair<Point,Integer> t:p){
							if(t.second.equals(centroid.id)){
								X+=t.first.x;
								Y+=t.first.y;
								n++;
							}
						}
						c.set(j, new Centroid(centroid.id,X/n,Y/n));
					}
				}
				for(int j=0;j<p.size();j++){
					push(p.get(j).toString());
				}
			}
			else{
			String[] tmp=edge.split(",");
			double a = Double.parseDouble(tmp[0]);
			double b = Double.parseDouble(tmp[1]);
			p.add(Pair.make(new Point(a, b),0));
			}
		}
	}

	// *************************************************************************
//  DATA TYPES
//*************************************************************************

/**
* A simple two-dimensional point.
*/
public static class Point implements Serializable {

	public double x, y;

	public Point() {}

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public Point add(Point other) {
		x += other.x;
		y += other.y;
		return this;
	}

	public Point div(long val) {
		x /= val;
		y /= val;
		return this;
	}

	public double euclideanDistance(Point other) {
		return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
	}

	public void clear() {
		x = y = 0.0;
	}

	@Override
	public String toString() {
		return x + " " + y;
	}
}

/**
* A simple two-dimensional centroid, basically a point with an ID.
*/
public static class Centroid extends Point {

	public int id;

	public Centroid() {}

	public Centroid(int id, double x, double y) {
		super(x,y);
		this.id = id;
	}

	public Centroid(int id, Point p) {
		super(p.x, p.y);
		this.id = id;
	}

	@Override
	public String toString() {
		return id + " " + super.toString();
	}
}
	
}



