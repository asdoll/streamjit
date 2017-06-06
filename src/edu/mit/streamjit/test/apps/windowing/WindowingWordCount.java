package edu.mit.streamjit.test.apps.windowing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.util.Pair;


public class WindowingWordCount {
	
	public static int window=250;
	public static int slide=150;

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/wordcount.in");
		try{
			window = Integer.parseInt(args[0]);
			slide = Integer.parseInt(args[1]);
		}catch(Exception e){
			System.out.println("no setting of window or slide, use default:window=250,slide=150");
			window=250;
			slide=150;
		}
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new WordCount();
		CompiledStream stream = sc.compile(core, in,
				Output.<String> toPrintStream(System.out));
		stream.awaitDrained();
		
long et = System.currentTimeMillis();
		
		System.out.println(bt);
		System.out.println(et);
	}

	public static class WordCount extends Filter<String, String> {
		
		public WordCount() {
			super(1, 1);
		}

		@Override
		public void work() {
			String s = pop();
			String[] words = s.toLowerCase().split("\\W+");
			Queue<String> q = new LinkedList<>();
			List<List<Pair<String,Integer>>> res = new ArrayList<>();
			int count = 0;
			for(String word:words){
				q.add(word);
				count++;
				if(count==slide){
					while(q.size()>window){
						q.poll();
					}
					{
						List<Pair<String,Integer>> t = new ArrayList<>();
						Hashtable<String,Integer> h=new Hashtable<>();
						int value;
						for(String g:q){
							if(h.get(g) != null){
								value = h.get(g);
								value++;
								h.remove(g);
								h.put(g, value);
							}else{
								h.put(g, 1);
							}
						}
						Enumeration e = h.keys();

						  while( e. hasMoreElements() ){
							  
						  String tmp = (String) e.nextElement();
						  t.add(new Pair<>(tmp,h.get(tmp)));

						  }
						  res.add(t);
					}
					count = 0;
				}
			}
			if(count!=0) {
				List<Pair<String,Integer>> t = new ArrayList<>();
				Hashtable<String,Integer> h=new Hashtable<>();
				int value;
				for(String g:q){
					if(h.get(g) != null){
						value = h.get(g);
						value++;
						h.remove(g);
						h.put(g, value);
					}else{
						h.put(g, 1);
					}
				}
				Enumeration e = h.keys();

				  while( e. hasMoreElements() ){
					  
				  String tmp = (String) e.nextElement();
				  t.add(new Pair<>(tmp,h.get(tmp)));

				  }
				  res.add(t);
			}
				
				for(List l:res){
					push(l.toString());
			  }

		}
				
	}
	
}