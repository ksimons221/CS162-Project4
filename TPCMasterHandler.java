/**
 * Handle TPC connections over a socket interface
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

import java.awt.image.RescaleOp;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.spec.MGF1ParameterSpec;

/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 * 
 */
public class TPCMasterHandler implements NetworkHandler {
	private KVServer kvServer = null;
	private ThreadPool threadpool = null;
	private TPCLog tpcLog = null;

	private long slaveID = -1;

	// Used to handle the "ignoreNext" message
	private boolean ignoreNext = false;

	// States carried from the first to the second phase of a 2PC operation
	private KVMessage originalMessage = null;
	private boolean aborted = true;

	public TPCMasterHandler(KVServer keyserver) {
		this(keyserver, 1);
	}

	public TPCMasterHandler(KVServer keyserver, long slaveID) {
		this.kvServer = keyserver;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(1);
	}

	public TPCMasterHandler(KVServer kvServer, long slaveID, int connections) {
		this.kvServer = kvServer;
		this.slaveID = slaveID;
		threadpool = new ThreadPool(connections);
	}

	public KVMessage getOrigMsg() {
		return this.originalMessage;
	}

	private class ClientHandler implements Runnable {
		private KVServer keyserver = null;
		private Socket client = null;

		private void closeConn() {
			try {
				client.close();
			} catch (IOException e) {
			}
		}

		@Override
		public void run() {

			KVMessage msg = null;
			try {
				InputStream myStream = client.getInputStream();
				msg = new KVMessage(myStream);
				client.shutdownInput();
			} catch (KVException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			// System.out.println("From coordinater sever: " + msg.toString());

			// Parse the message and do stuff

			String key = msg.getKey();

			if (msg.getMsgType().equals("putreq")) {

				if (ignoreNext == true) {
					sendAbortMessage(msg.getTpcOpId());
					ignoreNext = false;
					closeConn();
					return;
				} else {
					tpcLog.appendAndFlush(msg);
					handlePut(msg, key);
				}
			} else if (msg.getMsgType().equals("getreq")) {
				handleGet(msg, key);
			} else if (msg.getMsgType().equals("delreq")) {
				if (ignoreNext == true) {
					sendAbortMessage(msg.getTpcOpId());
					ignoreNext = false;
					closeConn();
					return;
				} else {
					tpcLog.appendAndFlush(msg);
					handleDel(msg, key);
				}
			} else if (msg.getMsgType().equals("ignoreNext")) {
				// Set ignoreNext to true. PUT and DEL handlers know what to do.
				// Implement me

				// Send back an acknowledgment
				// Implement me

				ignoreNext = true;

				sendIgnoreNextAck();

			} else if (msg.getMsgType().equals("commit") || msg.getMsgType().equals("abort")) {
				// Check in TPCLog for the case when SlaveServer is restarted
				// Implement me

				if (msg.getMsgType().equals("commit")) {
					aborted = false;

				} else {
					aborted = true;
				}

				handleMasterResponse(msg, originalMessage, aborted);

			}

			// Finally, close the connection
			closeConn();
		}

		private void sendAbortMessage(String id) {

			KVMessage toReturn = null;
			try {
				toReturn = new KVMessage("abort", "Ignore next");
			} catch (KVException e) {
				return;
			}
			toReturn.setTpcOpId(id);
			try {
				toReturn.sendMessage(client);
			} catch (KVException e) {
				return;
			}

		}

		private void sendIgnoreNextAck() {

			KVMessage toReturn = null;
			try {
				toReturn = new KVMessage("resp", "Success");
			} catch (KVException e) {
				return;
			}

			try {
				toReturn.sendMessage(client);
			} catch (KVException e) {
				return;
			}

		}

		private void sendToReturn(KVMessage toReturn) {
			try {
				toReturn.sendMessage(client);
			} catch (KVException e) {
				return;
			}
		}

		private void handlePut(KVMessage msg, String key) {
			AutoGrader.agTPCPutStarted(slaveID, msg, key);

			// Store for use in the second phase
			originalMessage = new KVMessage(msg);

			KVMessage response = null;
			try {
				response = new KVMessage("ready");
				response.setTpcOpId(originalMessage.getTpcOpId());
			} catch (KVException e) {
				AutoGrader.agTPCPutFinished(slaveID, msg, key);
				return;
			}

			try {
				response.sendMessage(client);
				AutoGrader.agTPCPutFinished(slaveID, msg, key);
				return;
			} catch (KVException e) {
				AutoGrader.agTPCPutFinished(slaveID, msg, key);
				return;
			}

		}

		private void handleGet(KVMessage msg, String key) {
			AutoGrader.agGetStarted(slaveID);

			String kvServerResponse = null;
			KVMessage toReturn = null;

			try {
				kvServerResponse = this.keyserver.get(key);
			} catch (KVException e) {
				sendToReturn(e.getMsg());
				AutoGrader.agGetFinished(slaveID);
				return;
			}

			try {
				toReturn = new KVMessage("resp");
			} catch (KVException e) {
				sendToReturn(e.getMsg());
				AutoGrader.agGetFinished(slaveID);
				return;
			}

			toReturn.setKey(key);
			toReturn.setValue(kvServerResponse);
			try {
				toReturn.sendMessage(client);
				AutoGrader.agGetFinished(slaveID);
				return;
			} catch (KVException e) {
				sendToReturn(e.getMsg());
				AutoGrader.agGetFinished(slaveID);
				return;
			}

		}

		private void handleDel(KVMessage msg, String key) {
			AutoGrader.agTPCDelStarted(slaveID, msg, key);
			System.out.println("in handleDel");

			// Store for use in the second phase
			originalMessage = new KVMessage(msg);

			String errorMsg = null;
			try {
				String present = this.keyserver.get(msg.getKey());
			} catch (KVException e1) {
				errorMsg = "Error";
				errorMsg = e1.getMsg().getMessage();
			}

			KVMessage response = null;
			if (errorMsg == null) {
				try {
					response = new KVMessage("ready");
					response.setTpcOpId(originalMessage.getTpcOpId());
				} catch (KVException e) {
					sendToReturn(e.getMsg());
					AutoGrader.agTPCDelFinished(slaveID, msg, key);
					return;
				}

			} else {
				try {
					response = new KVMessage("abort");
					response.setTpcOpId(originalMessage.getTpcOpId());
					response.setMessage(errorMsg);
				} catch (KVException e) {
					sendToReturn(e.getMsg());
					AutoGrader.agTPCDelFinished(slaveID, msg, key);
					return;

				}
			}

			try {
				response.sendMessage(client);
			} catch (KVException e) {
				sendToReturn(e.getMsg());
				AutoGrader.agTPCDelFinished(slaveID, msg, key);
				return;
			}

			AutoGrader.agTPCDelFinished(slaveID, msg, key);

		}

		/**
		 * Second phase of 2PC
		 * 
		 * @param masterResp
		 *            Global decision taken by the master
		 * @param origMsg
		 *            Message from the actual client (received via the
		 *            coordinator/master)
		 * @param origAborted
		 *            Did this slave server abort it in the first phase
		 */
		private void handleMasterResponse(KVMessage masterResp, KVMessage origMsg, boolean origAborted) {
			AutoGrader.agSecondPhaseStarted(slaveID, origMsg, origAborted);

			// /implement me
			if (origAborted == false) { // / do this shit
				if (origMsg == null) {
					KVMessage toReturn;
					try {
						toReturn = new KVMessage("ack");
						toReturn.setTpcOpId(masterResp.getTpcOpId());
						sendToReturn(toReturn);
					} catch (KVException e) {
						e.printStackTrace();
					}
				} else {
					if (origMsg.getMsgType().equals("putreq")) {
						try {
							this.keyserver.put(origMsg.getKey(), origMsg.getValue());
						} catch (KVException e) {
							System.out.println("Some how the put failed. BAD");
							return;
						}

						originalMessage = null;

						tpcLog.appendAndFlush(masterResp);

						try {
							KVMessage toReturn = new KVMessage("ack");
							toReturn.setTpcOpId(masterResp.getTpcOpId());
							sendToReturn(toReturn);
						} catch (KVException e) {
							e.printStackTrace();
						}
					} else if (origMsg.getMsgType().equals("delreq")) {
						try {
							this.keyserver.del(origMsg.getKey());
						} catch (KVException e) {
							System.out.println("Some how the del failed. BAD");
							return;
						}

						originalMessage = null;
						tpcLog.appendAndFlush(masterResp);

						KVMessage toReturn;
						try {
							toReturn = new KVMessage("ack");
							toReturn.setTpcOpId(masterResp.getTpcOpId());
							sendToReturn(toReturn);
						} catch (KVException e) {
							e.printStackTrace();
						}
					}
				}
			} else { // need to abort
				if (origMsg != null) { // / pairs to nothing
					tpcLog.appendAndFlush(masterResp);
				}
				originalMessage = null;
				KVMessage toReturn;
				try {
					toReturn = new KVMessage("ack");
					toReturn.setTpcOpId(masterResp.getTpcOpId());
					sendToReturn(toReturn);
				} catch (KVException e) {
					e.printStackTrace();
				}

			}

			AutoGrader.agSecondPhaseFinished(slaveID, origMsg, origAborted);
		}

		public ClientHandler(KVServer keyserver, Socket client) {
			this.keyserver = keyserver;
			this.client = client;
		}
	}

	@Override
	public void handle(Socket client) throws IOException {
		AutoGrader.agReceivedTPCRequest(slaveID);
		Runnable r = new ClientHandler(kvServer, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// TODO: HANDLE ERROR
			return;
		}
		AutoGrader.agFinishedTPCRequest(slaveID);
	}

	/**
	 * Set TPCLog after it has been rebuilt
	 * 
	 * @param tpcLog
	 */
	public void setTPCLog(TPCLog tpcLog) {
		this.tpcLog = tpcLog;
		this.originalMessage = tpcLog.getInterruptedTpcOperation();
	}

	/**
	 * Registers the slave server with the coordinator
	 * 
	 * @param masterHostName
	 * @param servr
	 *            KVServer used by this slave server (contains the hostName and
	 *            a random port)
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws KVException
	 */
	public void registerWithMaster(String masterHostName, SocketServer server) throws UnknownHostException, IOException, KVException {
		AutoGrader.agRegistrationStarted(slaveID);

		Socket master = new Socket(masterHostName, 9090);
		KVMessage regMessage = new KVMessage("register", slaveID + "@" + server.getHostname() + ":" + server.getPort());
		regMessage.sendMessage(master);

		// Receive master response.
		// Response should always be success, except for Exceptions. Throw away.

		KVMessage response = new KVMessage(master.getInputStream());
		System.out.println("Response back from server: " + response.toString());
		master.close();
		AutoGrader.agRegistrationFinished(slaveID);
	}
}
