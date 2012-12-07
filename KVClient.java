/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * This class is used to communicate with (appropriately marshalling and
 * unmarshalling) objects implementing the {@link KeyValueInterface}.
 * 
 */
public class KVClient implements KeyValueInterface {

	protected String server = null;
	protected int port = 0;

	/**
	 * @param server
	 *            is the DNS reference to the Key-Value server
	 * @param port
	 *            is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {
		this.server = server;
		this.port = port;
	}

	public Socket connectHost() throws KVException {
		Socket mySocket = null;
		try {
			mySocket = new Socket();
			//System.out.println("Server: " + server + "  port" + port);
			mySocket.connect(new InetSocketAddress(server, port));
		} catch (UnknownHostException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not connect");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not create socket");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		return mySocket;
	}

	public void closeHost(Socket sock) throws KVException {
		try {
			sock.close();
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Could not close port and socket");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
	}

	protected KVException transfomIOExcpetion(String excpetion) {
		KVMessage errorMessage = null;
		try {
			errorMessage = new KVMessage("resp", excpetion);
		} catch (KVException e) {
			return e;
		}
		return new KVException(errorMessage);
	}

	public Socket connectHost(int timeout) throws KVException {
		Socket mySocket = null;
		try {
			mySocket = new Socket();
			mySocket.connect(new InetSocketAddress(server, port), timeout);
		} catch (UnknownHostException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not connect");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (SocketTimeoutException e) {
			KVMessage errorMessage = new KVMessage("resp", "Socket timed out connecting to host");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not create socket");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		return mySocket;
	}

	public void put(String key, String value) throws KVException {
		Socket socket = connectHost();
		KVMessage ourMessage = new KVMessage("putreq");
		ourMessage.setKey(key);
		ourMessage.setValue(value);
		ourMessage.sendMessage(socket);

		// // After server processing
		InputStream myStream;
		KVMessage returnedMessage = null;
		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			throw e1;
		} catch (IOException e1) {
			throw transfomIOExcpetion("Network Error: Could not receive data");
		}

		closeHost(socket);

		if (returnedMessage.getMessage().equals("Success")) { // works no error
			//System.out.println("successfully put stuff");
			return;
		} else { // there was an error
			KVException toThrow = new KVException(returnedMessage);
			throw toThrow;
		}
	}

	public String get(String key) throws KVException {
		Socket socket = connectHost();
		//System.out.println("connected from get");
		KVMessage ourMessage = new KVMessage("getreq");
		ourMessage.setKey(key);
		ourMessage.sendMessage(socket);
		// // Sending back

		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			throw e1;
		} catch (IOException e1) {
			throw transfomIOExcpetion("Network Error: Could not receive data");
		}

		closeHost(socket);

		if (returnedMessage.getMessage() == null && returnedMessage.getValue() != null) {
			return returnedMessage.getValue();
		} else { // there was an error
			KVException toThrow = new KVException(returnedMessage);
			throw toThrow;
		}

	}

	public String get(String key, int timeout) throws KVException {
		Socket socket = connectHost(timeout);

		try {
			socket.setSoTimeout(timeout);
		} catch (SocketException e) {
			KVMessage errorMessage = new KVMessage("resp", "Socket timed out connecting to host");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}

		//System.out.println("connected from get");
		KVMessage ourMessage = new KVMessage("getreq");
		ourMessage.setKey(key);
		ourMessage.sendMessage(socket);
		// // Sending back

		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			throw e1;
		} catch (IOException e1) {
			throw transfomIOExcpetion("Network Error: Could not receive data");
		}

		closeHost(socket);

		if (returnedMessage.getMessage() == null && returnedMessage.getValue() != null) {
			return returnedMessage.getValue();
		} else { // there was an error
			KVException toThrow = new KVException(returnedMessage);
			throw toThrow;
		}

	}

	public void del(String key) throws KVException {
		Socket socket = connectHost();
		KVMessage ourMessage = new KVMessage("delreq");
		ourMessage.setKey(key);
		ourMessage.sendMessage(socket);
		// / Sending Back

		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			throw e1;
		} catch (IOException e1) {
			throw transfomIOExcpetion("Network Error: Could not receive data");
		}

		closeHost(socket);

		if (returnedMessage.getMessage().equals("Success")) { // works no error
			//System.out.println("Deleted del succsefully");
			return;
		} else { // there was an error
			KVException toThrow = new KVException(returnedMessage);
			throw toThrow;

		}
	}

	public void ignoreNext() throws KVException {
		Socket socket = connectHost();
		KVMessage ourMessage = new KVMessage("ignoreNext");
		ourMessage.sendMessage(socket);
		// / Sending Back

		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			throw e1;
		} catch (IOException e1) {
			throw transfomIOExcpetion("Network Error: Could not receive data");
		}

		closeHost(socket);

		if (returnedMessage.getMessage().equals("Success")) { // works no error
			return;
		} else { // there was an error
			KVException toThrow = new KVException(returnedMessage);
			throw toThrow;

		}
	}

	public String getServerName() {
		return this.server;
	}

	public int getPortName() {
		return this.port;
	}

	public KVMessage put(String key, String value, String TCPopID, int timeout) {
		Socket socket = null;
		try {
			socket = connectHost(timeout);
		} catch (KVException e2) {
			KVMessage temp = null;
			try {
				temp = new KVMessage("abort", "Encountered a Socket error. The slave server could have timed out.");
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			temp.setTpcOpId(TCPopID);
			try {
				closeHost(socket);
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			return temp;
		}

		try {
			socket.setSoTimeout(timeout);
		} catch (SocketException e) {
			KVMessage temp = null;
			try {
				temp = new KVMessage("abort", "Encountered a Socket error. The slave server could have timed out.");
			} catch (KVException e1) {
				e1.getMsg().setMsgType("abort");
				return e1.getMsg();
			}
			temp.setTpcOpId(TCPopID);
			try {
				closeHost(socket);
			} catch (KVException e1) {
				e1.getMsg().setMsgType("abort");
				return e1.getMsg();
			}
			return temp;
		}

		KVMessage ourMessage = null;
		try {
			ourMessage = new KVMessage("putreq");
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}

		ourMessage.setKey(key);
		ourMessage.setValue(value);
		ourMessage.setTpcOpId(TCPopID);
		try {
			ourMessage.sendMessage(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			try {
				closeHost(socket);
			} catch (KVException e2) {
				e2.getMsg().setMsgType("abort");
				return e2.getMsg();
			}
			return e.getMsg();
		}

		// / Sending Back
		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			e1.getMsg().setMsgType("abort");
			try {
				closeHost(socket);
			} catch (KVException e2) {
				e2.getMsg().setMsgType("abort");
				return e2.getMsg();
			}
			return e1.getMsg();
		} catch (IOException e1) {
			KVException temp = transfomIOExcpetion("Network Error: Could not receive data");
			temp.getMsg().setMsgType("abort");
			try {
				closeHost(socket);
			} catch (KVException e2) {
				e2.getMsg().setMsgType("abort");
				return e2.getMsg();
			}
			return temp.getMsg();
		}

		try {
			closeHost(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}

		return returnedMessage;
	}

	public KVMessage del(String key, String TCPopID, int timeout) {
		Socket socket = null;
		try {
			socket = connectHost(timeout);
		} catch (KVException e2) {
			KVMessage temp = null;
			try {
				temp = new KVMessage("abort", "Encountered a Socket error. The slave server could have timed out.");
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			temp.setTpcOpId(TCPopID);
			try {
				closeHost(socket);
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			return temp;
		}

		try {
			socket.setSoTimeout(timeout);
		} catch (SocketException e) {
			KVMessage temp = null;
			try {
				temp = new KVMessage("abort", "Encountered a Socket error. The slave server could have timed out.");
			} catch (KVException e1) {
				e1.getMsg().setMsgType("abort");
				return e1.getMsg();
			}
			temp.setTpcOpId(TCPopID);
			try {
				closeHost(socket);
			} catch (KVException e1) {
				e1.getMsg().setMsgType("abort");
				return e1.getMsg();
			}
			return temp;
		}

		KVMessage ourMessage = null;
		try {
			ourMessage = new KVMessage("delreq");
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}

		ourMessage.setKey(key);
		ourMessage.setTpcOpId(TCPopID);
		try {
			ourMessage.sendMessage(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}

		// / Sending Back
		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e1) {
			e1.getMsg().setMsgType("abort");
			return e1.getMsg();
		} catch (IOException e1) {
			KVException temp = transfomIOExcpetion("Network Error: Could not receive data");
			temp.getMsg().setMsgType("abort");
			return temp.getMsg();
		}

		try {
			closeHost(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}

		return returnedMessage;
	}

	public KVMessage decision(boolean abort, String TCPopID, int timeout) {

		Socket socket = null;

		try {
			socket = connectHost(timeout);
		} catch (KVException e2) {
			KVMessage temp = null;
			try {
				temp = new KVMessage("abort", "Encountered a Socket error. The slave server could have timed out.");
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			temp.setTpcOpId(TCPopID);
			try {
				closeHost(socket);
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
			return temp;
		}

		KVMessage ourMessage = null;
		if (abort) {
			try {
				ourMessage = new KVMessage("abort");
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
		} else {
			try {
				ourMessage = new KVMessage("commit");
			} catch (KVException e) {
				e.getMsg().setMsgType("abort");
				return e.getMsg();
			}
		}
		ourMessage.setTpcOpId(TCPopID);

		try {

			ourMessage.sendMessage(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}
		// // After Slave Server
		InputStream myStream;
		KVMessage returnedMessage = null;

		try {
			myStream = socket.getInputStream();
			returnedMessage = new KVMessage(myStream);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		} catch (IOException e1) {
			KVException temp = transfomIOExcpetion("Network Error: Could not receive data");
			temp.getMsg().setMsgType("abort");
			return temp.getMsg();
		}

		try {
			closeHost(socket);
		} catch (KVException e) {
			e.getMsg().setMsgType("abort");
			return e.getMsg();
		}
		return returnedMessage;

	}

}
