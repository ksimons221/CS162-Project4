/**
 * Sample instantiation of the Key-Value client  
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

import edu.berkeley.cs162.KVClient;

public class MultiClientTest {
	/**
	 * @param args
	 * @throws IOException
	 */
	//static KVServer key_server = null;
	static SocketServer server = null;
	protected static Thread mythreads[] = null;

	public static void main(String[] args) {

		int numOfClients  = 10;
		mythreads = new Thread[numOfClients];	
		
		for(int i =0; i< numOfClients; i++){
			mythreads[i] = new Thread(new runClient());
		}
		for(int i =0; i<numOfClients; i++){
			mythreads[i].start();
		}
		
		
	//	new runServer().start();
	/*	
		System.out.println("ALL CLEAR");
		runSpecificClient1 run1 = new runSpecificClient1();
		runSpecificClient2 run2 = new runSpecificClient2();
		run1.start();
		run2.run();
	*/
		
		//System.out.println(Server.key_server.getCache().toString());
	//	try {
	//		System.out.println(Server.key_server.getStore().toXML());
	//	} catch (KVException e) {
	//		e.printStackTrace();
	//	}
		
		//Server.server.stop();
		
		new runClient().start();
				
	}

}

class runClient extends Thread {

	public void run() {
		// random multiple thread tests
		KVClient kc = new KVClient("localhost", 8080);
		int num = (int) ((int) 100.0 * Math.random());
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			String three = "" + num;
			String seven = "" + (num + 1);

			String two = "" + (num - 1);
			String eight = "" + num;

			System.out.println("putting" + " " + three + " " + seven);
			kc.put(three, seven);
			boolean status = true;
			System.out.println("status: " + status);

			System.out.println("putting" + " " + two + " " + eight);
			kc.put(two, eight);
			System.out.println("status: " + status);

			System.out.println("getting key=2");
			String value = kc.get(two);
			System.out.println("returned: " + value);
			kc.del(three);
		} catch (KVException e) {
			System.out.println(e.getMsg().getMessage()); 
			e.printStackTrace();
		}

		// /Test to close Socket Server
		/*
		 * try{ Thread.sleep(4000);
		 * 
		 * }catch(Exception e){ e.printStackTrace();
		 * 
		 * } Server.server.stop();
		 */
	}
}

class runServer extends Thread {

	public void run() {
		try {
			Server.main(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
class runSpecificClient1 extends Thread {

	public void run() {
		try {
			Thread.sleep(3700);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		KVClient kc = new KVClient("localhost", 8080);
		int num = (int) ((int) 100.0 * Math.random());
		try {
			boolean status = true;
		    kc.put("amrit", "orange");
			kc.put("kevin", "brown");
			System.out.println("status" + status);
			kc.get("kevin");
			//kc.del("kevin");
		    

			
		} catch (Exception e) {
		   e.printStackTrace();
		}
	}

}
class runSpecificClient2 extends Thread {

	public void run() {
		KVClient kc = new KVClient("localhost", 8080);
		int num = (int) ((int) 100.0 * Math.random());
		try {
			Thread.sleep(4000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			kc.put("kevin", "white");
			kc.del("amrit");
		} catch (Exception e) {
			 e.printStackTrace();
		}
	}

}
