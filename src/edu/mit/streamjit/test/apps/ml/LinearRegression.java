package edu.mit.streamjit.test.apps.ml;

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

public class LinearRegression {
	
	static final int numbers=100;
	static List<Pair<Double,Double>> xy = new ArrayList<>();
	static double theta0=0,theta1=0;

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/LR.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new LR();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
//		System.out.println(bt);
//		System.out.println(et);
	}

	public static class LR extends Filter<String, String> {

		public LR() {
			super(Rate.create(1), Rate.create(0,2),Rate.create(0));
		}

		@Override
		public void work() {
			String edge = pop();
			if(edge.equals("END")){
				double X=0,Y=0;
				double XY=0,sumXsqr=0;
				for(int i=0;i<xy.size();i++){
					Pair<Double,Double> t = xy.get(i);
					X+=t.first;
					Y+=t.second;
					XY+=t.first*t.second;
					sumXsqr+=t.first*t.first;
				}
				X=X/xy.size();
				Y=Y/xy.size();
				XY-=xy.size()*X*Y;
				sumXsqr-=xy.size()*X*X;
				double b = XY/sumXsqr;
				double a = Y-b*X;
				push(Pair.make(a, b).toString());
			}
			else{
			String[] tmp=edge.split(",");
			double a = Double.parseDouble(tmp[0]);
			double b = Double.parseDouble(tmp[1]);
			xy.add(Pair.make(a, b));
			}
		}
	}

}