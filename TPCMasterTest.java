package edu.berkeley.cs162;

import junit.framework.TestCase;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.net.BindException;







public class TPCMasterTest extends TestCase {
TPCMaster coordinator;
String numberOfSlaves = "5";
//make slaveIds random numbers between 0 and 5000
int randomID = (int)(Math.random()*5000);
int[] slaveIds={1024, 800, 800, 1024, 3000};
int[] slavePorts={1025, 1026, 1027, 1025, 1028};
int[] slaves = {800, 1024, 3000};
boolean kvThrown;
String[] putKeys = {"5", "801", "1025", "2012"};
String[] putValues= {"5-value", "801-value", "1025-value", "2012-value"};


//TPCMaster test implicitly tests TPCMasterHandler


//current slaveIds are shouldHave[0], shouldhave[1], shouldhave[2]

	public void testAll() throws Exception {
		//SETUP, REGISTER SLAVES
	
		
	      String[] arguments = new String[] {numberOfSlaves};
	      ClaireServer.main(arguments);
	      this.coordinator = ClaireServer.tpcMaster;	

	      new TrialRegistration(this.slaveIds, this.slavePorts);
	      while(coordinator.stillRunningRegistration()){
	    	  System.out.println(".*");
	      }
	      System.out.println("done");
		

	/*
	 * expected behavior
	 * Slave 0: pass
	 * Slave 1: pass
	 * Slave 2: fail (duplicate ID)
	 * Slave 3: fail (duplicate port)
	 * slave 4: pass 
	 * 
	 */
		
	//TEST THAT REGISTRATION WORKED
		assertEquals(coordinator.getNumSlaves(), 3);
		boolean containsID=false;
		for (int i=0; i< this.slaves.length; i++){
			containsID=false;
			for (int j = 0; j< coordinator.getSlaveInfoArray().length; j++){
				if (coordinator.getSlaveInfoArray()[j]==this.slaves[i]){
					containsID=true;
				}
			}
			assertTrue(containsID);	
		
		}
		
	//TEST THAT PUT REQUEST WRITES TO PROPER SLAVES/CACHE
		coordinator.printSlaveInfo();
		KVMessage putRequest0 = new KVMessage("putreq");
		putRequest0.setKey(putKeys[0]);
		putRequest0.setValue(putValues[0]);
		putRequest0.setTpcOpId("1");
		KVMessage getRequest0 = new KVMessage("getreq");
		getRequest0.setKey(putKeys[0]);
		getRequest0.setTpcOpId("2");
		coordinator.performTPCOperation(putRequest0, true);
		System.out.println("we're getting from : "+ slaves[0]);
		assertEquals(coordinator.getFromSlave(slaves[1], putKeys[0]), putValues[0]);
		assertEquals(coordinator.getFromSlave(slaves[2], putKeys[0]), putValues[0]);
		assertEquals(coordinator.getFromCache(putKeys[0]), putValues[0]);
		//GET THE SAME VALUE, ASSERT THAT CAME FROM CACHE NOT SLAVES
		assertEquals(coordinator.handleGet(getRequest0), putValues[0]);
		assertFalse(coordinator.calledSlaves);
		//GET A KEY WE DON'T HAVE
		KVMessage getRequest1 = new KVMessage("getreq");
		getRequest1.setKey(putKeys[1]);
		getRequest1.setTpcOpId("3");
		boolean kvThrown = false;
		try{
		coordinator.handleGet(getRequest1);
		}
		catch(KVException e ){
			kvThrown=true;
		}
		assertTrue(kvThrown);
		
	//DELETE THE KEY WE PUT IN, MAKE SURE IT'S GONE FROM SLAVES AND CACHE
		KVMessage delRequest0 = new KVMessage("delreq");
		delRequest0.setKey(putKeys[0]);
		delRequest0.setTpcOpId("4");
		coordinator.performTPCOperation(delRequest0, false);
		kvThrown=false;
	//make sure slave 1 doesn't have it
		try{
			coordinator.getFromSlave(slaves[1], putKeys[0]);
		}
		catch (KVException d){
			kvThrown=true;
		}
		assertTrue(kvThrown);
		kvThrown=false;
	//make sure slave 2 doesn't have it
		try{
			coordinator.getFromSlave(slaves[2], putKeys[0]);
		}
		catch (KVException d){
			kvThrown=true;
		}
		assertTrue(kvThrown);
		
		kvThrown=false;
	//make sure cache doesn't have it
		try{
			coordinator.handleGet(getRequest0);
		}
		catch (KVException d){
			kvThrown=true;
		}
		assertTrue(kvThrown);	
	}
	


	
	public class TrialRegistration {
		/**
		 * @param args
		 */
		int[] ids;
		int[] ports;

		public TrialRegistration(int[] ids, int[] ports){
			for (int i=0; i < ids.length; i++){
				new Thread(new runSlaveServer(ids[i],ports[i])).start();	
			}
		}
		


		
		public class runSlaveServer implements Runnable {
			String masterHostName = "localhost";
			String logPath = null;
			TPCLog tpcLog = null;

			KVServer keyServer = null;
			SocketServer server = null;

			long slaveID = -1;
			int masterPort = 8080;
			int registrationPort = 9090;
			int port = 1; // /////// THIS IS CHANGED


			public runSlaveServer(int i, int j) {
				slaveID = i;
				port = j;		}

			public void run() {

				// Create TPCMasterHandler
				System.out.println("Binding SlaveServer with id: "+ slaveID);
				keyServer = new KVServer(100, 10);
				server = new SocketServer("localhost", port);
				TPCMasterHandler handler = new TPCMasterHandler(keyServer, slaveID);
				server.addHandler(handler);
				
				try {
					server.connect();
				} catch (IOException e) {
					System.out.println("COULD NOT CONNECT TO SOCKET");
				}

				// Create TPCLog
				logPath = slaveID + "@" + server.getHostname();
				tpcLog = new TPCLog(logPath, keyServer);

				// Load from disk and rebuild logs
	

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

				System.out.println("Starting SlaveServer at "
						+ server.getHostname() + ":" + server.getPort());
				
				try {
					server.run();
				} catch (IOException e) {
					e.printStackTrace();
				}
				  catch (NullPointerException n){
					  System.out.println("ERROR: IN TPCMASTER: server run causing nullpointer");
				  }
				
			}
		
		}
	}

}
