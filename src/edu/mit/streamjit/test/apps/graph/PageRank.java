package edu.mit.streamjit.test.apps.graph;

import java.nio.file.Path;
import java.nio.file.Paths;
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

public class PageRank {
	private static final double DAMPENING_FACTOR = 0.85;
	private static final double EPSILON = 0.0001;
	static final int numbers=16;
	static byte[][] graph = new byte[numbers+1][numbers+1];
	static double[] v = new double[numbers+1];

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/connectedcomponents.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new Rank();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
//		System.out.println(bt);
//		System.out.println(et);
	}

	public static class Rank extends Filter<String, String> {

		public Rank() {
			super(Rate.create(1), Rate.create(0,numbers),Rate.create(0));
		}

		@Override
		public void work() {
			String edge = pop();
			if(edge.equals("END")){
				boolean changed = true;
				for(int i=1;i<numbers+1;i++){
					v[i]=0.25;
				}
					for(int i=1;i<numbers+1;i++){
						v[i] = (1-DAMPENING_FACTOR)/numbers;
						for(int j=1;j<numbers+1;j++){
							if(graph[j][i]==1){
								int connected = connectedPages(j);
								v[i]+=DAMPENING_FACTOR*v[j]/connected;
								//v[j]=v[j]-v[j]/connected;
							}
						}
					}
				for(int i=1;i<numbers+1;i++){;
					push(Pair.make(i,v[i]).toString());
				}
			}
			else{
			String[] tmp=edge.split(",");
			int a = Integer.parseInt(tmp[0]);
			int b = Integer.parseInt(tmp[1]);
			graph[a][b]=1;
			}
		}
	}
	
	public static int connectedPages(int page){
		int s = 0;
		for(int i=1;i<numbers+1;i++){
			if(graph[page][i]==1) s++;
		}
		
		return s;
		
	}

}