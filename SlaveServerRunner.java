package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class SlaveServerRunner {

	/**
	 * @param args
	 */
	public static boolean logDebugging = true;

	public TPCLog getKVSlave(int index) {

		return runningSlaves.get(index).getTPCLog();
		
	}
	
	ArrayList<runSlaveServer1> runningSlaves = new ArrayList<runSlaveServer1>();
	
	public void run() {

		runSlaveServer1 temp1 = new runSlaveServer1(50, 4000);
		new Thread(temp1).start();

		runSlaveServer1 temp2 = new runSlaveServer1(50000, 4001);
		new Thread(temp2).start();

		runSlaveServer1 temp3 = new runSlaveServer1(12345, 4002);
		new Thread(temp3).start();

		runSlaveServer1 temp4 = new runSlaveServer1(22345, 4003);
		new Thread(temp4).start();

		runSlaveServer1 temp5 = new runSlaveServer1(42345, 4004);
		new Thread(temp5).start();
	}

	public class runSlaveServer1 implements Runnable {
		String masterHostName = "localhost";
		String logPath = null;
		TPCLog tpcLog = null;

		KVServer keyServer = null;
		SocketServer server = null;

		long slaveID = -1;
		int masterPort = 8080;
		int registrationPort = 9090;
		int port = 1; // /////// THIS IS CHANGED

		public runSlaveServer1() {

			slaveID = 1;
			port = 1;
		}

		public TPCLog getTPCLog() {
			return tpcLog;
		}

		public runSlaveServer1(int i, int j) {
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

			if (logDebugging == false) {
				try {
					tpcLog.rebuildKeyServer();
				} catch (KVException e) {
					e.printStackTrace();
				}
			}

			// Set log for TPCMasterHandler
			handler.setTPCLog(tpcLog);

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
