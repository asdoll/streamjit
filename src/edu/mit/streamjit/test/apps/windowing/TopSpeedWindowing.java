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

public class TopSpeedWindowing {
	static int evictionSec = 10;
	static double triggerMeters = 50;

	public static void main(String[] args) throws InterruptedException {
		long bt = System.currentTimeMillis();
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/cardata.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new CarPipeline();
		CompiledStream stream = sc.compile(core, in, Output.<String>toPrintStream(System.out));
		stream.awaitDrained();
long et = System.currentTimeMillis();
		
		System.out.println(bt);
		System.out.println(et);
		
	}

	public static Queue<Tuple4<Integer, Integer, Double, Long>> CarData1 = new LinkedList<>();
	static Double m1 = 0.0;
	static Double m2 = 0.0;
	public static Queue<Tuple4<Integer, Integer, Double, Long>> CarData2 = new LinkedList<>();

	public static class CarData extends Filter<String, String> {
		int num;

		public CarData(int number) {
			super(1, 1);
			num = number;
		}

		@Override
		public void work() {
			String s = pop();
			s = s.substring(1, s.length() - 1);
			String[] nums = s.split(",");
			Tuple4<Integer, Integer, Double, Long> current = new Tuple4<>(Integer.parseInt(nums[0]),
					Integer.parseInt(nums[1]), Double.parseDouble(nums[2]), Long.parseLong(nums[3]));
			Tuple4<Integer, Integer, Double, Long> top = new Tuple4<Integer, Integer, Double, Long>(0, 0, 0.0,
					(long) 0);
			//push(current.toString());
			if (current.f0==0) {
				CarData1.add(current);
				if (m1 == 0.0)
					m1 = current.f2;
				if (current.f2 - m1 >= triggerMeters) {
					top = current;
					for (Tuple4<Integer, Integer, Double, Long> g : CarData1) {
						if ((g.f1 > top.f1) && (current.f3 % 100 - CarData1.peek().f3 % 100 <= evictionSec)) {
							//push(g.toString());
							top = g;
						}
					}
					System.out.println(top.toString());
					//push(top.toString());
					m1 = 0.0;
					
				}
			}
			if (current.f0==1) {
				CarData2.add(current);
				if (m2 == 0.0)
					m2 = current.f2;
				if (current.f2 - m2 >= triggerMeters) {
					top = current;
					for (Tuple4<Integer, Integer, Double, Long> g : CarData2) {
						if ((g.f1 > top.f1) && (current.f3 % 100 - CarData2.peek().f3 % 100 <= evictionSec)) {
							//push(g.toString());
							top = g;
						}
					}
					System.out.println(top.toString());
					//push(top.toString());
					m2 = 0.0;
					
				}

			}
		}
	}

	private static final class CarPipeline extends Pipeline<String, String> {
		private CarPipeline() {
			Splitjoin<Float, Float> split = new Splitjoin<>(new DuplicateSplitter<Float>(),
					new RoundrobinJoiner<Float>());
			split.add(new Pipeline<Float, Float>(new CarData(1), new CarData(2)));
			add(split);
		}
	}
}