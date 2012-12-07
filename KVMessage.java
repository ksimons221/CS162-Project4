/**
 * XML Parsing library for the key-value store
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import java.io.*;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

/**
 * This is the object that is used to generate messages the XML based messages
 * for communication between clients and servers.
 */
public class KVMessage implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String status = null;
	private String message = null;
	private String tpcOpId = null;

	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getStatus() {
		return status;
	}

	public final void setStatus(String status) {
		this.status = status;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public String getMsgType() {
		return msgType;
	}

	public void setMsgType(String msgType) {
		this.msgType = msgType;
	}

	public String getTpcOpId() {
		return tpcOpId;
	}

	public void setTpcOpId(String tpcOpId) {
		this.tpcOpId = tpcOpId;
	}

	/*
	 * Solution from
	 * http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html
	 */
	private class NoCloseInputStream extends FilterInputStream {
		public NoCloseInputStream(InputStream in) {
			super(in);
		}

		public void close() {
		} // ignore close
	}

	/***
	 * 
	 * @param msgType
	 * @throws KVException
	 *             of type "resp" with message "Message format incorrect" if
	 *             msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
		if (validMessageType(msgType)) {
			this.msgType = msgType;
		} else {
			KVMessage response = new KVMessage("resp", "Unknown Error: msgType invalid");
			throw new KVException(response);
		}
	}

	public KVMessage(String msgType, String message) throws KVException {
		if (validMessageType(msgType)) {
			this.msgType = msgType;
		} else {
			KVMessage response = new KVMessage("resp", "Unknown Error: msgType invalid");
			throw new KVException(response);
		}
		this.message = message;
	}

	/***
	 * Parse KVMessage from incoming network connection
	 * 
	 * @param sock
	 * @throws KVException
	 *             if there is an error in parsing the message. The exception
	 *             should be of type "resp and message should be : a.
	 *             "XML Error: Received unparseable message" - if the received
	 *             message is not valid XML. b.
	 *             "Network Error: Could not receive data" - if there is a
	 *             network error causing an incomplete parsing of the message.
	 *             c. "Message format incorrect" - if there message does not
	 *             conform to the required specifications. Examples include
	 *             incorrect message type.
	 */

	public static String convertStreamToString(java.io.InputStream is) {
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
		return s.hasNext() ? s.next() : "";
	}

	public KVMessage(InputStream input1) throws KVException {
		String toReturn = convertStreamToString(input1);

		if (toReturn == null || toReturn.equals("")) {
			KVMessage errorMessage = new KVMessage("resp", "XML Error: Received unparseable message. The slave server could have timed out.");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		// System.out.println("Input stream string:" + toReturn);
		InputStream input = null;
		try {
			input = new ByteArrayInputStream(toReturn.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e2) {
		}

		DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuild;
		try {
			docBuild = fact.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw config exception. The slave server could have timed out.");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Document doc = null;

		try {
			doc = docBuild.parse(input);
		} catch (SAXException e) {
			KVMessage errorMessage = new KVMessage("resp", "XML Error: Received unparseable message. The slave server could have timed out. ");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not receive data. The slave server could have timed out.");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		this.msgType = doc.getElementsByTagName("KVMessage").item(0).getAttributes().getNamedItem("type").getNodeValue();

		NodeList tempKey = doc.getElementsByTagName("Key");
		if (tempKey.getLength() != 0) {
			if (tempKey.item(0).getFirstChild() == null) {
				this.key = null;
			} else {
				this.key = tempKey.item(0).getFirstChild().getNodeValue();
			}
		}
		NodeList tempValue = doc.getElementsByTagName("Value");
		if (tempValue.getLength() != 0) {
			if (tempValue.item(0).getFirstChild() == null) {
				this.value = null;
			} else {
				this.value = tempValue.item(0).getFirstChild().getNodeValue();
			}
		}
		NodeList tempMessage = doc.getElementsByTagName("Message");
		if (tempMessage.getLength() != 0) {
			if (tempMessage.item(0).getFirstChild() == null) {
				this.message = null;
			} else {
				this.message = tempMessage.item(0).getFirstChild().getNodeValue();
			}
		}
		NodeList tempMessageTPC = doc.getElementsByTagName("TPCOpId");
		if (tempMessageTPC.getLength() != 0) {
			if (tempMessageTPC.item(0).getFirstChild() == null) {
				this.tpcOpId = null;
			} else {
				this.tpcOpId = tempMessageTPC.item(0).getFirstChild().getNodeValue();
			}
		}

		checkValidity();
	}

	public void checkValidity() throws KVException {

		if (!validMessageType(this.msgType))
			throw new KVException(new KVMessage("resp", "Unknown Error: Message format incorrect"));

		if (this.msgType.equals("putreq")) {

			if (this.key == null || this.value == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;

			}
		} else if (this.msgType.equals("getreq")) {

			if (this.key == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;

			}
		} else if (this.msgType.equals("delreq")) {

			if (this.key == null || this.tpcOpId == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;

			}
		}

		else if (this.msgType.equals("resp")) {
			if ((message == null) && (this.getKey() == null || this.getValue() == null)) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;

			}
		}

		else if (this.msgType.equals("ready")) {
			if (this.tpcOpId == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;

			}
		}

		else if (this.msgType.equals("abort")) {
			if (this.tpcOpId == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		} else if (this.msgType.equals("register")) {
			if (this.message == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		} else if (this.msgType.equals("commit")) {
			if (this.tpcOpId == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}

		} else if (this.msgType.equals("ack")) {
			if (this.tpcOpId == null) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		} else if (this.msgType.equals("ignoreNext")) {

		} else {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Message format incorrect");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}

	}

	public static boolean validMessageType(String input) {
		return input.equals("getreq") || input.equals("putreq") || input.equals("ignoreNext") || input.equals("delreq") || input.equals("resp") || input.equals("register") || input.equals("ready") || input.equals("commit") || input.equals("abort") || input.equals("ack");
	}

	/**
	 * Copy constructor
	 * 
	 * @param kvm
	 */
	public KVMessage(KVMessage kvm) {
		this.msgType = kvm.msgType;
		this.key = kvm.key;
		this.value = kvm.value;
		this.status = kvm.status;
		this.message = kvm.message;
		this.tpcOpId = kvm.tpcOpId;
	}

	/**
	 * Generate the XML representation for this message.
	 * 
	 * @return the XML String
	 * @throws KVException
	 *             if not enough data is available to generate a valid KV XML
	 *             message
	 */
	public String toXML() throws KVException {
		try {
			checkValidity();
		} catch (KVException e) {
			throw e;
		}

		DocumentBuilderFactory dbfac = null;
		try {
			dbfac = DocumentBuilderFactory.newInstance();
		} catch (FactoryConfigurationError e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		DocumentBuilder docBuilder = null;

		try {
			docBuilder = dbfac.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Document doc = docBuilder.newDocument();

		doc.setXmlStandalone(true);
		Element kvm = null;
		try {
			kvm = doc.createElement("KVMessage");
			kvm.setAttribute("type", msgType);
			doc.appendChild(kvm);
		} catch (DOMException e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw DOM exception at type");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		if (this.key != null) {
			try {
				Element kXML = doc.createElement("Key");
				kvm.appendChild(kXML);
				Text keyText = doc.createTextNode(this.key);
				kXML.appendChild(keyText);
			} catch (DOMException e) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw DOM exception at key");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}
		if (this.value != null) {
			try {
				Element vXML = doc.createElement("Value");
				kvm.appendChild(vXML);
				Text vText = doc.createTextNode(this.value);
				vXML.appendChild(vText);
			} catch (DOMException e) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw DOM exception at value");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}
		if (this.message != null) {
			try {
				Element mXML = doc.createElement("Message");
				kvm.appendChild(mXML);
				Text mText = doc.createTextNode(this.message);
				mXML.appendChild(mText);
			} catch (DOMException e) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw DOM exception at value");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}
		if (this.tpcOpId != null) {
			try {
				Element tpcXML = doc.createElement("TPCOpId");
				kvm.appendChild(tpcXML);
				Text tpcText = doc.createTextNode(this.tpcOpId);
				tpcXML.appendChild(tpcText);
			} catch (DOMException e) {
				KVMessage errorMessage = new KVMessage("resp", "Unknown Error: XML Parser threw DOM exception at value");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}
		TransformerFactory transfac = null;
		try {
			transfac = TransformerFactory.newInstance();
		} catch (TransformerFactoryConfigurationError e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: TransformerFactory threw exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Transformer trans = null;
		try {
			trans = transfac.newTransformer();
		} catch (TransformerConfigurationException e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: Transformer threw exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		String xmlString = null;
		try {
			StringWriter sw = new StringWriter();
			StreamResult result = new StreamResult(sw);
			DOMSource source = new DOMSource(doc);
			trans.transform(source, result);
			xmlString = sw.toString();
		} catch (Exception e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: String converter threw exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		return xmlString;
	}

	public void sendMessage(Socket socket) throws KVException {

		OutputStream oos;
		try {
			oos = socket.getOutputStream();
		} catch (IOException e) {
			//System.out.println("IO EXCEPTION IN SEND MESSAGE IN KVMESSAGE");
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not send data");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		try {
			String ourXML = toXML();
			byte[] inBytes = ourXML.getBytes("UTF-8");
			oos.write(inBytes);
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Network Error: Could not send data");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (KVException kv) {
			throw kv;
		}

		try {
			socket.shutdownOutput();
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "Unknown Error: error closing output stream");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;

		}

	}

	@Override
	public String toString() {
		String toReturn = "The msgType: " + msgType + "   The message is:  " + message + "   The key is:  " + key + "   The value is:  " + value;
		return toReturn;
	}

}
