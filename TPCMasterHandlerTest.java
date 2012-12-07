package edu.berkeley.cs162;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import junit.framework.TestCase;

public class TPCMasterHandlerTest extends TestCase {
	// @Test(threadPoolSize = 3, invocationCount = 9, timeOut = 10000)
	public synchronized void testTPCMasterHandlerKVServer() {

		String logPath = null;
		TPCLog tpcLog = null;

		KVServer keyServer = null;
		SocketServer server = null;

		long slaveID = 360;
		int port = 4000; // /////// THIS IS CHANGED

		System.out.println("Binding SlaveServer:");
		keyServer = new KVServer(100, 10);
		server = new SocketServer("localhost", port);
		TPCMasterHandler handler = new TPCMasterHandler(keyServer, slaveID);
		server.addHandler(handler);
		try {
			server.connect();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Create TPCLog
		logPath = slaveID + "@" + server.getHostname();
		tpcLog = new TPCLog(logPath, keyServer);

		// Load from disk and rebuild logs

		try {
			tpcLog.rebuildKeyServer();
		} catch (KVException e1) {
			e1.printStackTrace();
		}

		// Set log for TPCMasterHandler
		handler.setTPCLog(tpcLog);

		System.out.println("Starting SlaveServer at " + server.getHostname() + ":" + server.getPort());

		Runnable r = new Runnable() {
			public void run() {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				KVClient kc = new KVClient("localhost", 4000);

				System.out.println("Start");
				String three = "three";
				String seven = "seven";
				KVMessage returned = kc.put("three", "seven", "1", 5000);
				//kc.put(three, seven);
				
				assertTrue(returned.getMsgType().equals("ready"));
				
				KVMessage slave1Commit = kc.decision(false, "1", 5000);
				while (slave1Commit.getMsgType().equals("ack") == false) {
					slave1Commit = kc.decision(false, "1", 5000);
				}
				assertTrue(slave1Commit.getMsgType().equals("ack"));
				
				Socket connectmySocket = null;
				try {
					connectmySocket = kc.connectHost(5000);
				} catch (KVException e) {
					e.printStackTrace();
				}
				KVMessage ignoreNextMessage = null;
				try {
					ignoreNextMessage = new KVMessage("ignoreNext");
					ignoreNextMessage.sendMessage(connectmySocket);
				} catch (KVException e) {
					e.printStackTrace();
				}
				
				InputStream myStream;
				KVMessage returnedMessage = null;
				try {
					myStream = connectmySocket.getInputStream();
					returnedMessage = new KVMessage(myStream);
				} catch (KVException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				assertTrue(returnedMessage.getMsgType().equals("resp"));
				assertTrue(returnedMessage.getMessage().equals("Success"));
				
				KVMessage returned2 = kc.put("three", "eight", "2", 5000);
				assertTrue(returned2.getMsgType().equals("abort"));
				
				KVMessage slave2Commit = kc.decision(true, "2", 5000);
			//	while (slave2Commit.getMsgType().equals("ack") == false) {
				//	slave2Commit = kc.decision(true, "2", 5000);
			//	}
				assertTrue(slave2Commit.getMsgType().equals("ack"));
				String returnedValue = null;
				try {
					returnedValue = kc.get("three", 50000);
				} catch (KVException e) {
					e.printStackTrace();
				}
				assertTrue(returnedValue.equals("seven"));
				//put 3 7
				//get ready
				// send commit
				// get ack
				
				// send ignoreNext
				// get back resp with message succsess
				
				// put 3 8
				// get back abort
				// send abort
				// get ack
				
				//get 3
				// shoudl get 7
				
			}
		};
		Thread runMessages = new Thread(r);
		
		
		abortPhase myServer = new abortPhase(server);
		Thread runServer = new Thread(myServer);
		runServer.start();
		runMessages.run();
		//Thread test =new Thread(new runClient2());
		
		//test.start();
		

	}
	private class abortPhase implements Runnable{
		SocketServer myServer;
		public abortPhase(SocketServer server){
			myServer = server;
			
		}
	
		public void run() {
			try {
				myServer.run();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
}
