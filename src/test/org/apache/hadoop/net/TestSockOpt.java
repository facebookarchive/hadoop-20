package org.apache.hadoop.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Before;
import org.junit.Test;

public class TestSockOpt {
	
	@Before
	public void checkLoaded() {
		assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
	}
	
	private ServerSocket startServerSocketAndAssert(int port, 
			int ipTOS) throws IOException {
		final ServerSocket serverSocket = new ServerSocket(port);
		
		new Thread() {
			@Override
			public void run() {
				try {
					Socket socket = serverSocket.accept();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} 
		}.start();
		
		return serverSocket;
	}
	
	private Socket createClientSocket(int port, 
									  int ipTosVal,
									  Socket socket) 
					throws IOException {
		InetAddress addr = InetAddress.getByName("localhost");
		SocketAddress sockAddr = new InetSocketAddress(addr, port);
		NetUtils.connect(socket, sockAddr, 1000, ipTosVal);
		return socket;
	}
	
	@Test
	public void testSetIPTOS() throws IOException {
		int ipTOS = 184;
		int port = 9091;
		ServerSocket serverSocket = null;
		Socket csocket = null;
		try {
			serverSocket = startServerSocketAndAssert(port, ipTOS);
			
			// test SocketChannel
			csocket = createClientSocket(port, ipTOS, 
							new StandardSocketFactory().createSocket());
			int actualTOS = NetUtils.getIPTOS(csocket.getChannel());
			assertEquals(ipTOS, actualTOS);
		} finally {
			if (null != csocket) {
				csocket.close();
			}
			if (null != serverSocket) {
				serverSocket.close();
			} 
		}
	}
	
	@Test
	public void testSetInvalidIPTOS() throws IOException {
		int ipTOS = 7;
		int port = 9092;
		ServerSocket serverSocket = null;
		Socket csocket = null;
		try {
			serverSocket = startServerSocketAndAssert(port, ipTOS);
			
			// test SocketChannel
			csocket = createClientSocket(port, ipTOS, 
							new StandardSocketFactory().createSocket());
			int actualTOS = NetUtils.getIPTOS(csocket.getChannel());
			assertEquals(4, actualTOS);
		} finally {
			if (null != csocket) {
				csocket.close();
			}
			if (null != serverSocket) {
				serverSocket.close();
			} 
		}
	}
	
}