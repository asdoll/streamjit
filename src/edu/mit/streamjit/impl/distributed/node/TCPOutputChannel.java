package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.TCPConnection;
import edu.mit.streamjit.impl.distributed.runtimer.ListenerSocket;

/**
 * This is {@link BoundaryOutputChannel} over TCP. Reads data from the given
 * {@link Buffer} and send them over the TCP connection.
 * <p>
 * Note: TCPOutputChannel acts as server when making TCP connection.
 * </p>
 * <p>
 * TODO: Need to aggressively optimise this class.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 29, 2013
 */
public final class TCPOutputChannel implements BoundaryOutputChannel {

	private Boolean debugPrint;

	private String name;

	private int portNo;

	private AtomicBoolean stopFlag;

	private boolean cleanStop;

	private Connection tcpConnection;

	private Buffer buffer;

	public TCPOutputChannel(Buffer buffer, int portNo, String bufferTokenName,
			Boolean debugPrint) {
		this.buffer = buffer;
		this.portNo = portNo;
		this.stopFlag = new AtomicBoolean(false);
		this.cleanStop = false;
		this.name = "TCPOutputChannel - " + bufferTokenName;
		this.debugPrint = debugPrint;
	}

	@Override
	public void closeConnection() throws IOException {
		tcpConnection.closeConnection();
	}

	@Override
	public boolean isStillConnected() {
		return (tcpConnection == null) ? false : tcpConnection
				.isStillConnected();
	}

	@Override
	public Runnable getRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				if (tcpConnection == null || !tcpConnection.isStillConnected()) {
					try {
						makeConnection();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				while (!stopFlag.get())
					sendData();

				if (cleanStop)
					finalSend();
				try {
					closeConnection();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
	}

	public void sendData() {
		while (this.buffer.size() > 0 && !stopFlag.get()) {
			try {
				if (debugPrint) {
					Object o = buffer.read();
					System.out.println(Thread.currentThread().getName() + " - "
							+ o.toString());
					tcpConnection.writeObject(o);
				} else
					tcpConnection.writeObject(buffer.read());
			} catch (IOException e) {
				System.err
						.println("TCP Output Channel. WriteObject exception.");
				reConnect();
			}
		}
	}

	@Override
	public int getOtherNodeID() {
		return 0;
	}

	@Override
	public void stop(boolean clean) {
		this.cleanStop = clean;
		this.stopFlag.set(true);
	}

	/**
	 * This can be called when running the application with the final scheduling
	 * configurations. Shouldn't be called when autotuner tunes.
	 */
	private void finalSend() {
		while (this.buffer.size() > 0) {
			try {
				// System.out.println(Thread.currentThread().getName() +
				// " buffer.size()" + this.buffer.size());
				if (debugPrint) {
					Object o = buffer.read();
					System.out.println(Thread.currentThread().getName()
							+ " FinalSend - " + o.toString());
					tcpConnection.writeObject(o);
				} else
					tcpConnection.writeObject(buffer.read());
			} catch (IOException e) {
				System.err.println("TCP Output Channel. finalSend exception.");
			}
		}
	}

	private void makeConnection() throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(portNo);
		Socket socket = listnerSckt.makeConnection(0);
		this.tcpConnection = new TCPConnection(socket);
	}

	private void reConnect() {
		ListenerSocket lstnSckt;
		try {
			lstnSckt = new ListenerSocket(portNo);
			this.tcpConnection.closeConnection();
			while (!stopFlag.get()) {
				System.out.println("TCPOutputChannel : Reconnecting...");
				try {
					Socket skt = lstnSckt.makeConnection(1000);
					this.tcpConnection = new TCPConnection(skt);
					return;
				} catch (SocketTimeoutException stex) {
					// We make this exception to recheck the stopFlag. Otherwise
					// thread will get struck at server.accept().
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	@Override
	public String name() {
		return name;
	}
}
