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

public class EnumTriangles {
	
	static final int numbers=16;
	static byte[][] graph = new byte[numbers+1][numbers+1];

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/enumtriangles.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new EnumTriangle();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
//		System.out.println(bt);
//		System.out.println(et);
	}

	public static class EnumTriangle extends Filter<String, String> {

		public EnumTriangle() {
			super(Rate.create(1), Rate.create(0,numbers),Rate.create(0));
		}

		@Override
		public void work() {
			String edge = pop();
			if(edge.equals("END")){
				for(int i=1;i<numbers+1;i++){;
					for(int j=i;j<numbers+1;j++){
						for(int k=j;k<numbers+1;k++){
							if((graph[i][j]==1) && (graph[k][j]==1) && (graph[i][k]==1))
								push(Tuple3.of(i, j, k).toString());
						}
						graph[i][j]=0;
					}
				
				//push(Pair.make(i,v[i]).toString());
				}
			}
			else{
			String[] tmp=edge.split(",");
			int a = Integer.parseInt(tmp[0]);
			int b = Integer.parseInt(tmp[1]);
			graph[a][b]=1;
			graph[b][a]=1;
			}
		}
	}

}