/**
 * Handle client connections over a socket interface
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2011, University of California at Berkeley
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
 *  DISCLAIMED. IN NO EVENT SHALL PRASHANTH MOHAN BE LIABLE FOR ANY
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
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections. It
 * uses a threadpool to ensure that none of it's methods are blocking.
 * 
 */
public class KVClientHandler implements NetworkHandler {
	private KVServer kvServer = null; // // ALWAYS NULL!!!!
	private ThreadPool threadpool = null;
	private TPCMaster tpcMaster = null;

	public KVClientHandler(KVServer keyserver, TPCMaster tpcMaster) {
		initialize(keyserver, 1, tpcMaster);
	}

	public KVClientHandler(KVServer keyserver, int connections, TPCMaster tpcMaster) {
		initialize(keyserver, connections, tpcMaster);
	}

	private void initialize(KVServer kvServer, int connections, TPCMaster tpcMaster) {
		this.kvServer = kvServer;
		threadpool = new ThreadPool(connections);
		this.tpcMaster = tpcMaster;
	}

	private class ClientHandler implements Runnable {
		private KVServer kvServer = null;
		private Socket client = null;

		private void sendToReturn(KVMessage toReturn) {
			try {
				toReturn.sendMessage(client);
			} catch (KVException e) {
				return;
			}
		}

		@Override
		public void run() {

			KVMessage myMessage = null;
			KVMessage toReturn = null;

			try {
				InputStream myStream = client.getInputStream();
				myMessage = new KVMessage(myStream);
				client.shutdownInput();
			} catch (KVException e1) {
				//e1.printStackTrace();
			} catch (IOException e1) {
				//e1.printStackTrace();
			}

			//System.out.println("From actual client: " + myMessage.toString());

			while (tpcMaster.allSlavesRegisteredAndAlive() == false) {
				
			}
			
			try {
				toReturn = new KVMessage("resp");
			} catch (KVException e1) {
				toReturn = e1.getMsg();
				sendToReturn(toReturn);
				return;

			}

			if (myMessage.getMsgType().equals("putreq")) {
				boolean putResponse = false;
				try {
					putResponse = tpcMaster.performTPCOperation(myMessage, true);
					//System.out.println("Response for the put: " + putResponse);
				} catch (KVException e) {
					//System.out.println(e.getMsg().toString());
					toReturn.setMessage(e.getMsg().getMessage());
					sendToReturn(toReturn);
					return;
				}
				toReturn.setMessage("Success");
				sendToReturn(toReturn);
				return;

			} else if (myMessage.getMsgType().equals("delreq")) {
				boolean delResponse = false;
				try {
					delResponse = tpcMaster.performTPCOperation(myMessage, false);
					//System.out.println("Response for the del: " + delResponse);
				} catch (KVException e) { // / write back error
					//System.out.println(e.getMsg().toString());
					toReturn.setMessage(e.getMsg().getMessage());
					sendToReturn(toReturn);
					return;
				}
				toReturn.setMessage("Success");
				sendToReturn(toReturn);
				return;
			} else if (myMessage.getMsgType().equals("getreq")) {
				try {
					String result = tpcMaster.handleGet(myMessage);
					if (result == null) {
						//System.out.println("SHOULD NEVER EQUAL NULL. SHOULD BE KVEXCPETION INSTEAD");
					}
					toReturn.setKey(myMessage.getKey());
					toReturn.setValue(result);
					sendToReturn(toReturn);
					return;
				} catch (KVException e) {
					sendToReturn(e.getMsg());
					return;
				}
			} else {
				//System.out.println("Bad mgs type in kv client handler");
			}

		}

		public ClientHandler(KVServer kvServer, Socket client) {
			this.kvServer = kvServer;
			this.client = client;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.berkeley.cs162.NetworkHandler#handle(java.net.Socket)
	 */
	@Override
	public void handle(Socket client) throws IOException {
		Runnable r = new ClientHandler(kvServer, client);
		try {
			threadpool.addToQueue(r);
		} catch (InterruptedException e) {
			// Ignore this error
			return;
		}
	}
}
