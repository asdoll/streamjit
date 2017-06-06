package edu.mit.streamjit.test.apps.twitter;

import java.nio.file.Path;
import java.nio.file.Paths;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.compiler2.Compiler2StreamCompiler;

/**
 * This example demonstrate how to use Text file input in StreamJIT.
 * 
 * @author Sumanaruban Rajadurai
 * @since 21 Feb 2017
 */
public class UseTextFileInput {

	public static void main(String[] args) throws InterruptedException {
		StreamCompiler sc = new Compiler2StreamCompiler();
		Path path = Paths.get("data/tweets.in");
		Input<String> in = Input.fromTextFile(path);
		OneToOneElement<String, String> core = new SampleFilter();
		CompiledStream stream = sc.compile(core, in,
				Output.<String> toPrintStream(System.out));
		stream.awaitDrained();
	}

	public static class SampleFilter extends Filter<String, String> {

		public SampleFilter() {
			super(1, 1);
		}

		@Override
		public void work() {
			String s = pop() + "_TEXT";
			push("1");
			
		}
	}
}