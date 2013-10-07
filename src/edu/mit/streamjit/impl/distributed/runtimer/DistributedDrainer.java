package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	Controller controller;

	public DistributedDrainer(Controller controller) {
		this.controller = controller;
		DrainProcessor dp = new CNDrainProcessorImpl(this);
		controller.setDrainProcessor(dp);
	}

	@Override
	protected void drainingDone() {
		controller.drainingFinished();
	}

	@Override
	protected void drain(Token blobID, boolean isFinal) {
		controller.drain(blobID, isFinal);
	}

	@Override
	protected void drainingDone(Token blobID) {
		// Nothing to clean in Distributed case.
	}
}
