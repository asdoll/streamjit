package org.mit.jstreamit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Programmers building a stream graph can either create instances of Splitjoin
 * for one-off pipelines, or create subclasses of Splitjoin that create and pass
 * SteamElement instances to the superclass constructor.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public class Splitjoin<I, O> implements StreamElement<I, O> {
	//We'd like this to be a Splitter<I, T>, but that would require introducing
	//T as a type variable in Splitjoin.
	private final Splitter splitter;
	private final Joiner joiner;
	private final List<StreamElement<?, ?>> elements;
	public <T, U> Splitjoin(Splitter<I, T> splitter, Joiner<U, O> joiner, StreamElement<? super T, ? extends U>... elements) {
		int elems = elements.length;
		int splitOuts = splitter.supportedOutputs();
		int joinIns = joiner.supportedInputs();
		//If the splitter and joiner want different numbers of inputs and
		//outputs, and one of them isn't allowing any number, the combination is
		//invalid.
		if (splitOuts != joinIns && (splitOuts != Splitter.UNLIMITED || joinIns != Joiner.UNLIMITED))
			throw new IllegalArgumentException(String.format("Splitter produces %d outputs but joiner consumes %d inputs", splitOuts, joinIns));
		//TODO: these checks must be deferred until we're ready to go
//		if (splitOuts != Splitter.UNLIMITED && splitOuts != elems)
//			throw new IllegalArgumentException(String.format("Splitter expects %d outputs but %d elements provided", splitOuts, elems));
//		if (joinIns != Joiner.UNLIMITED && joinIns != elems)
//			throw new IllegalArgumentException(String.format("Joiner expects %d inputs but %d elements provided", joinIns, elems));
		this.splitter = splitter;
		this.joiner = joiner;
		this.elements = new ArrayList<>(elems);
		this.elements.addAll(Arrays.asList(elements));
	}

	public void add(StreamElement<?, ?> first, StreamElement<?, ?>... more) {
		elements.add(first);
		elements.addAll(Arrays.asList(more));
	}
}
