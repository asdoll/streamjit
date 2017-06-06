package edu.mit.streamjit.test.apps.windowing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.util.Pair;

public class SessionWindowing {

	public static Long windowTime = 3L;
	
	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/session.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new Session();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
		long et = System.currentTimeMillis();
		System.out.println(bt);
		System.out.println(et);
	}

	public static Hashtable<String,Pair<Long,Integer>> h =new Hashtable<>();
	public static List<String> names = new ArrayList<>();
	public static Long latestTimestamp = 0L;
	public static String latestName = "";

	public static class Session extends Filter<String, String> {

		public Session() {
			super(1, 1);
			
		}

		@Override
		public void work() {
			String s = pop();
			if(s.equals("end")){
						for(String nm:names){
							Pair<Long,Integer> tmp = h.get(nm);
							push('('+nm+','+tmp.first.toString()+','+tmp.second.toString()+")");
							h.remove(nm);
						}names.clear();
			}else{
			s = s.substring(1, s.length() - 1);
			String[] nums = s.split(",");
			String name = "";
			Long timestamp = 0L;
			Integer num = 0;
			name = nums[0].trim();
			timestamp = Long.parseLong(nums[1].trim());
		    num = Integer.parseInt(nums[2].trim());
		    if(timestamp-latestTimestamp>=windowTime){
				for(String nm:names){
					Pair<Long,Integer> tmp = h.get(nm);
					push('('+nm+','+tmp.first.toString()+','+tmp.second.toString()+")");
					h.remove(nm);
				}names.clear();
			}
			if(h.get(name) != null){
				Pair<Long,Integer> tmp = h.get(name);
				if(latestName.equals(name)){
					tmp = new Pair<Long,Integer>(tmp.first,tmp.second+1);
				}else{
					if(tmp.first>timestamp){
						tmp = new Pair<Long,Integer>(timestamp,num);
					}
				}
				h.remove(name);
				h.put(name, tmp);
			}else{
				names.add(name);
				Pair<Long,Integer> tmp = new Pair<Long,Integer>(timestamp,num);
				h.put(name, tmp);
			}
			latestName = name;
			latestTimestamp = timestamp;
		}
		}
	}
}