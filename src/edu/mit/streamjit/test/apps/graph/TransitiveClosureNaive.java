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

public class TransitiveClosureNaive {
	
	static final int numbers=16;
	static byte[][] graph = new byte[numbers+1][numbers+1];
	static int[] v = new int[numbers+1];

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/connectedcomponents.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new ConnectedComponent();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
//		System.out.println(bt);
//		System.out.println(et);
	}

	public static class ConnectedComponent extends Filter<String, String> {

		public ConnectedComponent() {
			super(Rate.create(1), Rate.create(0,numbers*(numbers-1)/2),Rate.create(0));
		}

		@Override
		public void work() {
			String edge = pop();
			if(edge.equals("END")){
				boolean changed = true;
				for(int i=1;i<numbers+1;i++){
					v[i]=i;
				}
				while(changed){
					changed = false;
					for(int i=1;i<numbers+1;i++){
						for(int j=i+1;j<numbers+1;j++){
							if(graph[i][j]==1)
							for(int k=j+1;k<numbers+1;k++){
							if(graph[j][k]==1)
								if(graph[i][k]!=1){
									graph[i][k]=1;
									changed = true;
							}
							}
						}
					}
				}
				for(int i=1;i<numbers+1;i++){
					for(int j=i+1;j<numbers+1;j++){
						if(graph[i][j]==1){
							graph[i][j]=0;
							push(Pair.make(i,j).toString());
						}
					}
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

}