package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import edu.berkeley.cs162.SlaveServerRunner.runSlaveServer1;

public class registerSingleSlave {

	int port = -1;
	int hostname = -1;

	public registerSingleSlave(int x, int y) {
		port = x;
		hostname = y;
	}

	public void run() {

		runSlaveServer1Single temp1 = new runSlaveServer1Single(port, hostname);
		new Thread(temp1).start();
	}

	public class runSlaveServer1Single implements Runnable {
		String masterHostName = "localhost";
		String logPath = null;
		TPCLog tpcLog = null;

		KVServer keyServer = null;
		SocketServer server = null;

		long slaveID = -1;
		int masterPort = 8080;
		int registrationPort = 9090;
		int port = 1; // /////// THIS IS CHANGED

		public TPCLog getTPCLog() {
			return tpcLog;
		}

		public runSlaveServer1Single(int i, int j) {
			slaveID = i;
			port = j;
		}

		public void run() {

			// Create TPCMasterHandler
			System.out.println("Binding SlaveServer:");
			keyServer = new KVServer(100, 10);
			try {
				server = new SocketServer(InetAddress.getLocalHost().getHostAddress(), port);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} // /// USED TO HAVE NO PORT
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
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			System.out.println("Original message after rebuild: " + handler.getOrigMsg());

			// Set log for TPCMasterHandler
			handler.setTPCLog(tpcLog);
			
			System.out.println("Original message after set: " + handler.getOrigMsg());

			// Register with the Master. Assuming it always succeeds (not
			// catching).
			try {
				handler.registerWithMaster(masterHostName, server);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (KVException e) {
				e.printStackTrace();
			}

			System.out.println("Starting SlaveServer at " + server.getHostname() + ":" + server.getPort());
			try {
				server.run();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
