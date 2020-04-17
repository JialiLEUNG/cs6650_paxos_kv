package com.github.jiali.paxos.utils.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class NonBlockClientImpl implements PaxosClient {
	// Reasons for using Selector: http://tutorials.jenkov.com/java-nio/selectors.html
	// Switching between threads is expensive for an operating system,
	// and each thread takes up some resources (memory) in the operating system too.
	// The advantage of using just a single thread to handle multiple channels is that
	// you need less threads to handle the channels.
	private Selector selector;
	private ConcurrentMap<SocketChannel, ByteBuffer> channelMap;
	private BlockingQueue<SocketChannel> channalQueue;

	public NonBlockClientImpl() throws IOException {
		super();
		// Create a Selector
		this.selector = Selector.open();
		this.channelMap = new ConcurrentHashMap<>();
		this.channalQueue = new LinkedBlockingQueue<>();
		new Thread(() -> {
			while (true) {
				try {
					this.selector.select();
					SocketChannel newChan = this.channalQueue.poll();
					if (newChan != null) {
						// Register Channels with the Selector
						// In order to use a Channel with a Selector
						// you must register the Channel with the Selector.
						// a channel that has connected successfully to another server
						newChan.register(this.selector, SelectionKey.OP_CONNECT);
					}
					// Once you have called one of the select() methods
					// and its return value has indicated that one or more channels are ready,
					// you can access the ready channels via the "selected key set"
					Set<SelectionKey> keys = this.selector.selectedKeys();
					// iterate the selected key set to access the ready channels
					Iterator<SelectionKey> iterator = keys.iterator();
					while (iterator.hasNext()) {
						SelectionKey key = iterator.next();
						SocketChannel channel = (SocketChannel) key.channel();
						// a connection was established with a remote server.
						if (key.isConnectable()) {
							try {
								if (channel.finishConnect()) {
									channel.register(this.selector, SelectionKey.OP_WRITE);
								}
							} catch (IOException e) {
								
							}
						} else if (key.isWritable()) { // a channel is ready for writing
							writeData(channel);
						}
						// the keyIterator.remove() call at the end of each iteration.
						// The Selector does not remove the SelectionKey instances from the selected key set itself.
						// You have to do this, when you are done processing the channel.
						iterator.remove();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * write data from the buffer to nonblocking channel
	 * 
	 * @param channel
	 * @throws IOException
	 */
	private void writeData(SocketChannel channel) throws IOException {
		ByteBuffer buffer = this.channelMap.get(channel);
		if (buffer == null) {
			return;
		}
		if (buffer.hasRemaining()) {
			// Write data to a SocketChannel
			// There is no guarantee of how many bytes the write() method writes to the SocketChannel.
			// Therefore we repeat the write() call until the Buffer has no further bytes to write.
			channel.write(buffer);
		}
		if (!buffer.hasRemaining()) {
			this.channelMap.remove(channel);
			channel.close();
		}
	}

	/**
	 * write data into buffer
	 * switch buffer from writing mode to reading mode
	 * and then create channel
	 * @param ip
	 * @param port
	 * @param msg
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	@Override
	public void sendTo(String ip, int port, byte[] msg) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		// Java NIO Buffers are used when interacting with NIO Channels.
		// Data is read from channels into buffers, and written from buffers into channels.
		// A buffer is essentially a block of memory into which you can write data,
		// which you can then later read again.
		// This memory block is wrapped in a NIO Buffer object,
		// which provides a set of methods that makes it easier to work with the memory block.

		// Allocate a Buffer
		ByteBuffer buffer = ByteBuffer.allocate(msg.length);
		// Write Data to a Buffer
		buffer.put(msg);
		// switch a Buffer from writing mode to reading mode
		buffer.flip();
		// A Java NIO SocketChannel is a channel that is connected to a TCP network socket.
		// open a SocketChannel
		SocketChannel socketChannel = SocketChannel.open();
		// set a SocketChannel into non-blocking mode
		socketChannel.configureBlocking(false);
		this.channelMap.put(socketChannel, buffer);
		this.channalQueue.add(socketChannel);
		// create connection
		SocketAddress address = new InetSocketAddress(ip, port);
		socketChannel.connect(address);
		this.selector.wakeup();
	}

	public static void main(String[] args) {
		try {
			PaxosClient client = new NonBlockClientImpl();
			client.sendTo("localhost", 8888, "hello ".getBytes());
			client.sendTo("localhost", 8888, "world!".getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
