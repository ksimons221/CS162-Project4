/**
 * Master for Two-Phase Commits
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.spec.MGF1ParameterSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//import edu.berkeley.cs162.KVClientHandler.ClientHandler;

public class TPCMaster {

	/**
	 * Implements NetworkHandler to handle registration requests from
	 * SlaveServers.
	 * 
	 */

	
	public KVMessage transformOtherException(String s) throws KVException {
		KVMessage temp = new KVMessage("resp", s);
		return temp;
	}

	private class ServerHandler implements Runnable {
		private Socket client = null;

		private void sendToReturn(KVMessage toReturn) {
			try {
				toReturn.sendMessage(client);
			} catch (KVException e) {
				return;
			}
		}

		public String slaveValidation(SlaveInfo newSlave) {

			int aliveSlaves = 0;
			for (int i = 0; i < theSlaveInfo.size(); i++) {
				if (newSlave.port == theSlaveInfo.get(i).port && theSlaveInfo.get(i).alive == true) {
					return "This port has alreadyt been assigned a slave and slave is alive";
				}
				if (newSlave.slaveID == theSlaveInfo.get(i).slaveID && theSlaveInfo.get(i).alive == true) {
					return "This slaveID has already been assigned a slave and slave is alive";
				}
				if (theSlaveInfo.get(i).alive == true) {
					aliveSlaves++;
				}
			}

			if (aliveSlaves == theSlaveInfo.size() && theSlaveInfo.size() == numSlaves) {
				return "The maximum number of slaves has alread registered";
			}

			return null;
		}

		@Override
		public void run() {
			KVMessage myMessage = null;
			try {
				InputStream myStream = client.getInputStream();
				myMessage = new KVMessage(myStream);
				client.shutdownInput();
			} catch (KVException e1) {
				sendToReturn(e1.getMsg());
				return;
			} catch (IOException e1) {
				try {
					sendToReturn(transformOtherException("Encountered an IO Exception"));
				} catch (KVException e) {
					sendToReturn(e.getMsg());
				}
				return;
			}

			SlaveInfo newSlave = null;

			if (myMessage.getMsgType().equals("register")) {
				try {
					newSlave = new SlaveInfo(myMessage.getMessage());
				} catch (KVException e) {
					sendToReturn(e.getMsg());
					return;
				}
			} else {
				System.out.println("The registration server is not getting a regiester request o_O");
				return;
			}
			String errorMessage = slaveValidation(newSlave);

			if (errorMessage != null) {
				KVMessage temp;
				try {
					temp = new KVMessage("resp", errorMessage);
					sendToReturn(temp);
					return;
				} catch (KVException e) {
					sendToReturn(e.getMsg());
					return;
				}
			}

			try {
				KVMessage temp = new KVMessage("resp", "Successfully registered " + newSlave.slaveID + "@" + newSlave.hostName + ":" + newSlave.port);
				sendToReturn(temp);
				boolean reRegisterd = backFromTheDead(newSlave);
				if (reRegisterd == false) {
					addSlave(newSlave);
				}
				System.out.println("Amount of registered slaves:" + theSlaveInfo.size());
				return;
			} catch (KVException e) {
				System.out.println("Amount of registered slaves:" + theSlaveInfo.size());
				sendToReturn(e.getMsg());
				return;
			}

		}

		public ServerHandler(Socket client) {
			this.client = client;

		}
	}

	private class TPCRegistrationHandler implements NetworkHandler {

		private ThreadPool threadpool = null;

		public TPCRegistrationHandler() {
			this(1);
		}

		public TPCRegistrationHandler(int connections) {
			threadpool = new ThreadPool(connections);
		}

		@Override
		public void handle(Socket client) throws IOException {

			Runnable r = new ServerHandler(client);
			try {
				threadpool.addToQueue(r);
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	/**
	 * Data structure to maintain information about SlaveServers
	 * 
	 */
	private class SlaveInfo {
		// 64-bit globally unique ID of the SlaveServer
		private long slaveID = -1;
		// Name of the host this SlaveServer is running on
		private String hostName = null;
		// Port which SlaveServer is listening to
		private int port = -1;

		private boolean alive = false;

		// Variables to be used to maintain connection with this SlaveServer
		private KVClient kvClient = null;

		// //private Socket kvSocket = null; changed in spec

		/**
		 * 
		 * @param slaveInfo
		 *            as "SlaveServerID@HostName:Port"
		 *            0@fgjlkfjsdlkajfsdal:85565
		 * @throws KVException
		 */
		public SlaveInfo(String slaveInfo) throws KVException {
			try {
				slaveInfosRegistered++;
				this.slaveID = Long.parseLong(slaveInfo.split("@")[0]);
				String afterTheAt = slaveInfo.split("@")[1];
				this.hostName = afterTheAt.split(":")[0];
				this.port = Integer.parseInt(afterTheAt.split(":")[1]);
				kvClient = new KVClient(hostName, port);
				alive = true;
			} catch (Exception e) {
				KVMessage temp = new KVMessage("resp", "Registration Error: Received unparseable slave information");
				throw new KVException(temp);
			}
		}

		public long getSlaveID() {
			return slaveID;
		}

		public KVClient getKvClient() {
			return kvClient;
		}

		@Override
		public String toString() {
			String toReturn = "The slave ID: " + slaveID + "   The hostName is:  " + hostName + "   The port is:  " + port;
			return toReturn;
		}
	}

	int response = 0;
	
	Lock myResponse = new ReentrantLock();
	Lock masterLockTCP = new ReentrantLock();
	Condition masterThread = masterLockTCP.newCondition();
    KVMessage [] MessageResponses = new  KVMessage[2];
    SlaveInfo [] slaveInfoResponses = new SlaveInfo[2];
    
	
	// Timeout value used during 2PC operations
	private static final int TIMEOUT_MILLISECONDS = 5000;

	// /FOR CLAIRE
	public int slaveInfosRegistered = 0;

	// Cache stored in the Master/Coordinator Server
	private KVCache masterCache = new KVCache(100, 10);

	// Registration server that uses TPCRegistrationHandler
	private SocketServer regServer = null;

	// Number of slave servers in the system
	private int numSlaves = -1;

	// ID of the next 2PC operation
	private Long tpcOpId = 0L;

	ArrayList<SlaveInfo> theSlaveInfo;

	ReentrantReadWriteLock.WriteLock kvSlaveServerLock = null;

	/**
	 * Creates TPCMaster
	 * 
	 * @param numSlaves
	 *            number of expected slave servers to register
	 * @throws Exception
	 */
	public TPCMaster(int numSlaves) {
		// Using SlaveInfos from command line just to get the expected number of
		// SlaveServers
		this.numSlaves = numSlaves;

		
		
		// // Lock stuff

		ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
		kvSlaveServerLock = masterLock.writeLock();
		


		// Create registration server
		theSlaveInfo = new ArrayList<SlaveInfo>();
		regServer = new SocketServer("localhost", 9090);
		regServer.addHandler(new TPCRegistrationHandler());

		try {
			regServer.connect();
		} catch (IOException e) {
		}
	}

	public boolean backFromTheDead(SlaveInfo newSlave) {

		for (int i = 0; i < theSlaveInfo.size(); i++) {
			if (newSlave.slaveID == theSlaveInfo.get(i).slaveID && theSlaveInfo.get(i).alive == false) {
				theSlaveInfo.set(i, newSlave);
				return true;
			}
		}
		return false;
	}

	/**
	 * Calculates tpcOpId to be used for an operation. In this implementation it
	 * is a long variable that increases by one for each 2PC operation.
	 * 
	 * @return
	 */
	private String getNextTpcOpId() {
		tpcOpId++;
		return tpcOpId.toString();
	}

	private void addSlave(SlaveInfo newSlave) {
		for (int i = 0; i < theSlaveInfo.size(); i++) {
			if (newSlave.getSlaveID() < theSlaveInfo.get(i).getSlaveID()) {
				theSlaveInfo.add(i, newSlave);
				return;
			}
		}
		System.out.println("The new biggest slave id is:  " + newSlave.getSlaveID());
		theSlaveInfo.add(newSlave);

	}

	/**
	 * Start registration server in a separate thread
	 */
	public void run() {
		AutoGrader.agTPCMasterStarted();

		Runnable r = new Runnable() {
			public void run() {
				try {
					regServer.run();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};

		Thread regThread = new Thread(r);
		regThread.start();
		AutoGrader.agTPCMasterFinished();
	}

	/**
	 * Converts Strings to 64-bit longs Borrowed from
	 * http://stackoverflow.com/questions
	 * /1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
	 * Adapted from String.hashCode()
	 * 
	 * @param string
	 *            String to hash to 64-bit
	 * @return
	 */
	private long hashTo64bit(String string) {
		// Take a large prime
		long h = 1125899906842597L;
		int len = string.length();

		for (int i = 0; i < len; i++) {
			h = 31 * h + string.charAt(i);
		}
		return h;
	}

	/**
	 * Compares two longs as if they were unsigned (Java doesn't have unsigned
	 * data types except for char) Borrowed from
	 * http://www.javamex.com/java_equivalents/unsigned_arithmetic.shtml
	 * 
	 * @param n1
	 *            First long
	 * @param n2
	 *            Second long
	 * @return is unsigned n1 less than unsigned n2
	 */
	private boolean isLessThanUnsigned(long n1, long n2) {
		return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
	}

	private boolean isLessThanEqualUnsigned(long n1, long n2) {
		return isLessThanUnsigned(n1, n2) || n1 == n2;
	}

	/**
	 * Find first/primary replica location
	 * 
	 * @param key
	 * @return
	 */

	public boolean keyValidation(String key) {
		if (key == null) {
			return false;
		}
		if (key == "") {
			return false;
		}
		return true;
	}

	public boolean allSlavesRegisteredAndAlive() {
		if (theSlaveInfo.size() != this.numSlaves) {
			return false;
		}
		for (int i = 0; i < theSlaveInfo.size(); i++) {
			if (theSlaveInfo.get(i).alive == false) {
				return false;
			}
		}
		return true;
	}

	private SlaveInfo findFirstReplica(String key) {
		// 64-bit hash of the key

		if (theSlaveInfo.size() == 1) {
			return theSlaveInfo.get(0);
		}

		long hashedKey = hashTo64bit(key.toString());

		for (int i = 0; i < theSlaveInfo.size(); i++) {
			if (hashedKey > theSlaveInfo.get(i).getSlaveID()) { // the next one
				return theSlaveInfo.get(i + 1);
			}
		}

		return theSlaveInfo.get(0);
	}

	/**
	 * Find the successor of firstReplica to put the second replica
	 * 
	 * @param firstReplica
	 * @return
	 */
	private SlaveInfo findSuccessor(SlaveInfo firstReplica) {
		// implement me
		for (int i = 0; i < theSlaveInfo.size(); i++) {
			if (firstReplica.getSlaveID() == theSlaveInfo.get(i).getSlaveID()) { // found
				if (i + 1 == theSlaveInfo.size()) { // last one
					return theSlaveInfo.get(0);
				} else {
					return theSlaveInfo.get(i + 1);
				}
			}
		}
		System.out.println("HORRIBLE IN FIND SUCCESSOR");
		return null;
	}

	/**
	 * Synchronized method to perform 2PC operations one after another
	 * 
	 * @param msg
	 * @param isPutReq
	 * @return True if the TPC operation has succeeded
	 * @throws KVException
	 */
	public synchronized boolean performTPCOperation(KVMessage msg, boolean isPutReq) throws KVException {
		AutoGrader.agPerformTPCOperationStarted(isPutReq);

		if (keyValidation(msg.getKey()) == false) { // bad key
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
			throw new KVException(new KVMessage("resp", "key null or empty formatted key"));
		}

		kvSlaveServerLock.lock();
		masterCache.getWriteLock(msg.getKey()).lock();
		SlaveInfo primary = findFirstReplica(msg.getKey());
		SlaveInfo secondary = findSuccessor(primary);
		
		String tempID1 = getNextTpcOpId();
		Thread firstServer = new Thread( new readyPhase(primary, isPutReq, tempID1, msg));
		Thread secondServer = new Thread( new readyPhase(secondary, isPutReq, tempID1, msg));
		 masterLockTCP.lock();

		firstServer.start();
		secondServer.start();
		
		 try {
			masterThread.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		 masterLockTCP.unlock();
	
	
		if (MessageResponses[0].getMsgType().equals("ready") && MessageResponses[1].getMsgType().equals("ready")) {
			
			
			Thread firstServer1 = new Thread( new commitPhase(primary, tempID1));
			Thread secondServer1 = new Thread( new commitPhase(secondary, tempID1));
			 masterLockTCP.lock();

			firstServer1.start();
			secondServer1.start();
			
			 try {
				masterThread.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 masterLockTCP.unlock();
			
			if (isPutReq == true) {
				masterCache.put(msg.getKey(), msg.getValue());
			} else {
				masterCache.del(msg.getKey());
			}
			masterCache.getWriteLock(msg.getKey()).unlock();
			kvSlaveServerLock.unlock();
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
			return true;
		} else { // slave server wants to abort
			System.out.println("Slave server wants to abort");
			
			
			
			Thread firstServer1 = new Thread( new abortPhase(primary, tempID1));
			Thread secondServer1 = new Thread( new abortPhase(secondary, tempID1));
			 masterLockTCP.lock();

			firstServer1.start();
			secondServer1.start();
			
			 try {
				masterThread.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 masterLockTCP.unlock();
			
			
			
			String errorMessage = "";
			if (MessageResponses[0].getMsgType().equals("ready") == false) {
				errorMessage = errorMessage + "@" + slaveInfoResponses[0].slaveID + ":=" + MessageResponses[0].getMessage();
			}
			if (MessageResponses[1].getMsgType().equals("ready") == false) {
				errorMessage = errorMessage + "\n@" + slaveInfoResponses[1].slaveID + ":=" + MessageResponses[1].getMessage();
			}
			KVMessage errorKVMessagep = new KVMessage("resp", errorMessage);
			KVException temp = new KVException(errorKVMessagep);
			masterCache.getWriteLock(msg.getKey()).unlock();
			kvSlaveServerLock.unlock();
			AutoGrader.agPerformTPCOperationFinished(isPutReq);
			throw temp;
		}

	}

	
	
	private class readyPhase implements Runnable {
		
		SlaveInfo primary = null;
		boolean isPutReq = false; 
		KVMessage msg = null;
		String tempID1 = null;
		public readyPhase(SlaveInfo mySlave, boolean putReq, String TCPopID, KVMessage myMsg){
			
			primary = mySlave;
			isPutReq = putReq;
			tempID1 = TCPopID;
			msg = myMsg;
		}
		
		public void run(){
			KVMessage slaveReady = null;
			if (isPutReq == true) {
				slaveReady = primary.getKvClient().put(msg.getKey(), msg.getValue(), tempID1, TIMEOUT_MILLISECONDS);
			} else {
				slaveReady = primary.getKvClient().del(msg.getKey(), tempID1, TIMEOUT_MILLISECONDS);
			}
			
			
			 myResponse.lock();
			 
			 if(response == 0){
				 MessageResponses[0] = slaveReady;
				 slaveInfoResponses[0] = primary;
				 response = 1;
			 }else{
				 MessageResponses[1] = slaveReady;
				 slaveInfoResponses[1] = primary; 
				 response = 0;
				 masterLockTCP.lock();
				 masterThread.signal();
				 masterLockTCP.unlock();

			 }
			 myResponse.unlock();
			
		}
		
	}
	
	

	private class commitPhase implements Runnable {
		
		SlaveInfo primary = null;
		String tempID1 = null;
		public commitPhase(SlaveInfo mySlave, String TCPop){
			primary = mySlave;
			tempID1 = TCPop;
		}
		
		public void run(){
			KVMessage slave1Commit = primary.getKvClient().decision(false, tempID1, TIMEOUT_MILLISECONDS);
			while (slave1Commit.getMsgType().equals("ack") == false) {
				slave1Commit = primary.getKvClient().decision(false, tempID1, TIMEOUT_MILLISECONDS);
			}
			
			
			 myResponse.lock();
			 if(response == 0){
				 response = 1;
			 }else{
				 response = 0;
				 masterLockTCP.lock();
				 masterThread.signal();
				 masterLockTCP.unlock();
			 }
			 myResponse.unlock();
			
		}
	}
	
private class abortPhase implements Runnable {
		
		SlaveInfo primary = null;
		String tempID1 = null;
		public abortPhase(SlaveInfo mySlave, String TCPop){
			primary = mySlave;
			tempID1 = TCPop;
		}
		
		public void run(){
			
			KVMessage slave1Commit = primary.getKvClient().decision(true, tempID1, TIMEOUT_MILLISECONDS);
			while (slave1Commit.getMsgType().equals("ack") == false) {
				slave1Commit = primary.getKvClient().decision(true, tempID1, TIMEOUT_MILLISECONDS);
			}
			
			 myResponse.lock();
			 if(response == 0){
				 response = 1;
			 }else{
				 response = 0;
				 masterLockTCP.lock();
				 masterThread.signal();
				 masterLockTCP.unlock();
			 }
			 myResponse.unlock();
			
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * Perform GET operation in the following manner: - Try to GET from
	 * first/primary replica - If primary succeeded, return Value - If primary
	 * failed, try to GET from the other replica - If secondary succeeded,
	 * return Value - If secondary failed, return KVExceptions from both
	 * replicas
	 * 
	 * @param msg
	 *            Message containing Key to get
	 * @return Value corresponding to the Key
	 * @throws KVException
	 */

	public String handleGet(KVMessage msg) throws KVException {
		AutoGrader.aghandleGetStarted();

		if (keyValidation(msg.getKey()) == false) { // bad key
			AutoGrader.aghandleGetFinished();
			throw new KVException(new KVMessage("resp", "key null or empty formatted key"));
		}
		
		masterCache.getWriteLock(msg.getKey()).lock();

		String cacheResult = masterCache.get(msg.getKey());
		
		if (cacheResult == null) { // not in cache

			SlaveInfo primary = findFirstReplica(msg.getKey());

			System.out.println("Asking first slave");

			String server1Response = null;

			try {
				server1Response = primary.getKvClient().get(msg.getKey(), TIMEOUT_MILLISECONDS);
			} catch (KVException k) {
				server1Response = null;
			}
			System.out.println("server1Response:  " + server1Response);

			if (server1Response == null) { // have to check second

				System.out.println("Asking second slave");

				SlaveInfo secondary = findSuccessor(primary);

				String server2Response = secondary.getKvClient().get(msg.getKey(), TIMEOUT_MILLISECONDS);

				if (server2Response == null) {

					AutoGrader.aghandleGetFinished();

					masterCache.getWriteLock(msg.getKey()).unlock();

					System.out.println("BOTH SERVERS ARE NULL");

					throw new KVException(new KVMessage("resp", "Error Message: The key could not be found"));

				} else {

					AutoGrader.aghandleGetFinished();

					masterCache.put(msg.getKey(), server2Response);

					masterCache.getWriteLock(msg.getKey()).unlock();
					
					return server2Response;
				}

			} else {

				AutoGrader.aghandleGetFinished();

				masterCache.put(msg.getKey(), server1Response);

				masterCache.getWriteLock(msg.getKey()).unlock();

				return server1Response;
			}

		} else { // in cache

			AutoGrader.aghandleGetFinished();

			masterCache.getWriteLock(msg.getKey()).unlock();
			
			return cacheResult;
		}

	}

	public boolean stillRunningRegistration() {
		boolean toReturn = this.slaveInfosRegistered < numSlaves;
		return toReturn;
	}

	public long[] getSlaveInfoArray() {
		long[] toReturn = new long[numSlaves];
		int i = 0;
		for (Iterator<TPCMaster.SlaveInfo> iterator = theSlaveInfo.iterator(); iterator.hasNext();) {
			SlaveInfo currentSlave = iterator.next();
			toReturn[i] = currentSlave.getSlaveID();
			i++;
		}
		return toReturn;
	}

	public void printSlaveInfo() {
		int i = 0;
		for (Iterator<TPCMaster.SlaveInfo> iterator = theSlaveInfo.iterator(); iterator.hasNext();) {
			SlaveInfo currentSlave = iterator.next();
			System.out.println("slave id " + i + ": " + currentSlave.getSlaveID());
			i++;
		}
	}

	public ArrayList<SlaveInfo> getSlaveInfo() {
		return theSlaveInfo;
	}


}
