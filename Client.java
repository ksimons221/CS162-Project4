/**
 * Sample instantiation of the Key-Value client  
 * 
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

public class Client {

	//// MUST RUN SERVER FIRST   ///// 
	
	public static void main(String[] args) throws IOException {
		KVClient kc = new KVClient("localhost", 8080);

		try {
			String three = "three";
			String seven = "seven";
			System.out.println("putting (3, 7)");
			kc.put(three, seven);
			System.out.println("putting (3, 8)");
			kc.put(three, "eight");
			System.out.println("getting key=3"); 
			String value = kc.get(three);
			System.out.println("returned: " + value);
		
			System.out.println("deleting key=3"); 
			kc.del(three);
			
			System.out.println("putting (7, 9)");
			kc.put("seven", "nine");
			
			
			System.out.println("getting key=3"); 
			String value2 = kc.get(three);
			System.out.println("returned: " + value2);
			

			
		} catch (KVException e) {
			System.out.println("KV EXCPETION CAME OUT");
			System.out.println(e.getMsg().toString());
		} catch (Exception e) {
		}

	}
}
