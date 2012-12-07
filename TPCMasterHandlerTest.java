package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.TestCase;

public class TPCMasterHandlerTest extends TestCase {

	public void testTPCMasterHandlerKVServer() {

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
				System.out.println("what up");

				KVClient kc = new KVClient("localhost", 4000);

				System.out.println("Start");
				String three = "three";
				String seven = "seven";
				System.out.println("putting (3, 7)");
				try {
					kc.put(three, seven);
				} catch (KVException e) {
					System.out.println("error in put");
				}
				boolean temp = true;
				int counter = 0;
				System.out.println("fdsfsd");
				while (temp) {
					// System.out.println("counter:  " + counter);
				}

			}
		};
		//Thread runMessages = new Thread(r);
		//runMessages.start();
		
		Thread test =new Thread(new runClient2());

		test.start();
		try {
			server.run();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	class runClient2 extends Thread {

		public void run() {
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("what up");

			KVClient kc = new KVClient("localhost", 4000);

			System.out.println("Start");
			String three = "three";
			String seven = "seven";
			System.out.println("putting (3, 7)");
			try {
				kc.put(three, seven);
			} catch (KVException e) {
				System.out.println("error in put");
			}
			boolean temp = true;
			int counter = 0;
			System.out.println("fdsfsd");
			while (temp) {
				// System.out.println("counter:  " + counter);
			}
		}
	}
}
