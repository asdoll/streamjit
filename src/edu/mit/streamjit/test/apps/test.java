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
	public static void main(String[] args) throws InterruptedException {
		// Benchmarker.runBenchmarks(new FMRadioBenchmarkProvider(), new DebugStreamCompiler()).get(0).print(System.out);
		StreamCompiler sc = new Compiler2StreamCompiler();
		OneToOneElement<String, Integer> fmradio = new TestPipeline();
		
		Path path = Paths.get("data/test.in");
		Input<String> input = Input.fromTextFile(path);
		CompiledStream cs = sc.compile(fmradio, input, Output.toPrintStream(System.out));
		cs.awaitDrained();
	}
	
	
	private static final class TestPipeline extends Pipeline<String, Integer> {
		private TestPipeline() {
			add(new Example1());
			add(new Example2());
//			Splitjoin<Integer, Integer> sj = new Splitjoin<Integer,Integer>(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>());
//			sj.add(new Example3());
//			sj.add(new Example4());
//			add(sj);
		}
	}
	
	private static final class Example1 extends Filter<String, Integer> {

		public Example1() {
			super(Rate.create(50),Rate.create(50),Rate.create(0));
			
		}

		@Override
		public void work() {
			Random a=new Random();
			for(int i=0;i<50;i++){
				if(a.nextFloat()<0.5){
					int t = Integer.parseInt(pop());
					if(a.nextFloat()<0.2){
						push(t);
					}
				}
			}
			if(a.nextFloat()<0.5){
				push(51);
			}
		}
		
	}
	private static final class Example2 extends Filter<Integer, Integer> {
		private Example2() {
			super(Rate.create(50),Rate.create(50),Rate.create(0));
		}

		@Override
		public void work() {
			Random a=new Random();
			for(int i=0;i<50;i++){
				if(a.nextFloat()<0.3){
					int t = pop();
					if(a.nextFloat()<0.5){
						push(t+1);
					}
				}
			}
			push(61);
		}
	}
	private static final class Example3 extends Filter<Integer, Integer> {

		public Example3() {
			super(Rate.create(0,20),Rate.create(0,31),Rate.create(0));
			
		}

		@Override
		public void work() {		
			Random a=new Random();
			a.setSeed(new Date().getTime());
			for(int i=0;i<20;i++){
				if(a.nextFloat()>0.5){
					int t = pop();
					push(t);
				}
					push(i);
			}
			push(71);
		}
		
	}
			
	private static final class Example4 extends Filter<Integer, Integer> {

		public Example4() {
			super(Rate.create(0,30),Rate.create(0,61),Rate.create(0));
			
		}

		@Override
		public void work() {		
			Random a=new Random();
			a.setSeed(new Date().getTime());
			for(int i=0;i<30;i++){
				if(a.nextFloat()>0.5){
					int t = pop();
					if(a.nextFloat()>0.5){
						push(t);
						push(t+1);
					}
				}
			}
			push(81);
		}
		
	}	
	
}
