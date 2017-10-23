/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.test.apps;

import com.google.common.collect.Collections2;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.jeffreybosboom.serviceproviderprocessor.ServiceProvider;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;
import edu.mit.streamjit.test.AbstractBenchmark;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.Datasets;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * http://benchmarksgame.alioth.debian.org/u32/performance.php?test=fannkuchredux#about
 * for details.  It would be faster to use int[] rather than List<Integer> but
 * that would be less convenient.  (Maybe we could even unbox it, by noticing we
 * never mutate the list size.)
 *
 * Note that this stream doesn't produce an output, but instead accumulates a
 * result in the Max filter.  Also note that we don't want to fuse the filters
 * because we can data-parallelize the first one.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class test {
	static final int MAX_ROUND = 100;
	
	public static void main(String[] args) throws InterruptedException {
		// Benchmarker.runBenchmarks(new FMRadioBenchmarkProvider(), new DebugStreamCompiler()).get(0).print(System.out);
		StreamCompiler sc = new Compiler2StreamCompiler();
		OneToOneElement<String, Integer> fmradio = new Compare();
		
		Path path = Paths.get("data/test.in");
		Input<String> input = Input.fromTextFile(path);
		CompiledStream cs = sc.compile(fmradio, input, Output.toPrintStream(System.out));
		cs.awaitDrained();
	}
	
	
	private static final class Compare extends Pipeline<String, Integer> {
		private Compare() {
			Splitjoin<String, Integer> sj = new Splitjoin<String,Integer>(new RoundrobinSplitter<String>(), new RoundrobinJoiner<Integer>());
			sj.add(new Less());
			sj.add(new Larger());
			sj.add(new EqualPipline());
			add(sj);
		}
	}
	
	private static final class Less extends Filter<String, Integer> {

		public Less() {
			super(Rate.create(0,MAX_ROUND),Rate.create(0,MAX_ROUND),Rate.create(0));
			
		}

		@Override
		public void work() {
			Random seed = new Random();
			seed.setSeed(System.currentTimeMillis());
			int number=seed.nextInt(MAX_ROUND);
			for(int i=0;i<number;i++){
				push(Integer.parseInt(pop())-1);
			}
		}
		
	}
	private static final class Larger extends Filter<String, Integer> {

		public Larger() {
			super(Rate.create(0,MAX_ROUND),Rate.create(0,MAX_ROUND),Rate.create(0));
			
		}

		@Override
		public void work() {
			Random seed = new Random();
			seed.setSeed(System.currentTimeMillis());
			int number=seed.nextInt(MAX_ROUND);
			for(int i=0;i<number;i++){
				push(Integer.parseInt(pop())+1);
			}
		}
		
	}
	private static final class EqualPipline extends Pipeline<String, Integer> {
		private EqualPipline() {
			Splitjoin<String, Integer> sj = new Splitjoin<String,Integer>(new RoundrobinSplitter<String>(), new RoundrobinJoiner<Integer>());
			sj.add(new Smaller());
			sj.add(new Bigger());
			add(sj);
		}
	}
	private static final class Smaller extends Filter<String, Integer> {
		public Smaller() {
			super(Rate.create(0,MAX_ROUND),Rate.create(0,MAX_ROUND),Rate.create(0));
			
		}

		@Override
		public void work() {
			Random seed = new Random();
			seed.setSeed(System.currentTimeMillis());
			int number=seed.nextInt(MAX_ROUND);
			for(int i=0;i<number;i++){
				push(Integer.parseInt(pop())/2);
			}
		}
	}
	private static final class Bigger extends Filter<String, Integer> {

		public Bigger() {
			super(Rate.create(0,MAX_ROUND),Rate.create(0,MAX_ROUND),Rate.create(0));
			
		}

		@Override
		public void work() {
			Random seed = new Random();
			seed.setSeed(System.currentTimeMillis());
			int number=seed.nextInt(MAX_ROUND);
			for(int i=0;i<number;i++){
				push(Integer.parseInt(pop())*2);
			}
		}
		
	}
	
}
