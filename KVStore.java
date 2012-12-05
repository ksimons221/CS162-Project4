/**
 * Persistent Key-Value storage layer. Current implementation is transient, 
 * but assume to be backed on disk when you do your project.
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

import java.util.Dictionary;
import java.util.Hashtable;

import java.util.Enumeration;
import java.io.*;
import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

/**
 * This is a dummy KeyValue Store. Ideally this would go to disk, or some other
 * backing store. For this project, we simulate the disk like system using a
 * manual delay.
 * 
 */
public class KVStore implements KeyValueInterface {
	private Dictionary<String, String> store = null;

	public KVStore() {
		resetStore();
	}

	private void resetStore() {
		store = new Hashtable<String, String>();
	}

	private KVException CreateIOExcpetion(String message) {

		KVMessage msg;
		try {
			msg = new KVMessage("resp", message);
		} catch (KVException e) {
			return e;
		}
		return new KVException(msg);

	}

	public void put(String key, String value) throws KVException {
		AutoGrader.agStorePutStarted(key, value);

		try {
			putDelay();
			store.put(key, value);
			return;
		} catch (Exception e) {
			throw CreateIOExcpetion("IO Error");
		} finally {
			AutoGrader.agStorePutFinished(key, value);
		}
	}

	public String get(String key) throws KVException {
		AutoGrader.agStoreGetStarted(key);

		try {
			getDelay();
			String retVal = this.store.get(key);
			if (retVal == null) {
				KVMessage msg = new KVMessage("resp", "Does not exist");
				throw new KVException(msg);
			}
			return retVal;
		} catch (KVException e) {
			throw e;
		} catch (Exception e) {
			throw CreateIOExcpetion("IO Error");
		} finally {
			AutoGrader.agStoreGetFinished(key);
		}
	}

	public void del(String key) throws KVException {
		AutoGrader.agStoreDelStarted(key);

		try {
			delDelay();
			if (key != null && this.store.get(key) != null) {
				this.store.remove(key);
			} else {
				KVMessage msg = new KVMessage("resp", "Does not exist");
				throw new KVException(msg);
			}
		} catch (KVException e) {
			throw e;
		} catch (Exception e) {
			throw CreateIOExcpetion("IO Error");
		} finally {
			AutoGrader.agStoreDelFinished(key);
		}
	}

	private void getDelay() {
		AutoGrader.agStoreDelay();
	}

	private void putDelay() {
		AutoGrader.agStoreDelay();
	}

	private void delDelay() {
		AutoGrader.agStoreDelay();
	}

	public String toXML() throws KVException {
		DocumentBuilderFactory dbfac = null;
		try {
			dbfac = DocumentBuilderFactory.newInstance();
		} catch (FactoryConfigurationError e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		DocumentBuilder docBuilder = null;
		try {
			docBuilder = dbfac.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Document doc = docBuilder.newDocument();
		doc.setXmlStandalone(true);
		Element kvs = null;
		try {
			kvs = doc.createElement("KVStore");
			doc.appendChild(kvs);
		} catch (DOMException e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw DOM exception at type");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		for (Enumeration<String> e = store.keys(); e.hasMoreElements();) {
			String key = e.nextElement();
			Element kvp = null;
			try {
				kvp = doc.createElement("KVPair");
				kvs.appendChild(kvp);

			} catch (DOMException f) {
				KVMessage errorMessage = new KVMessage("resp",
						"Unknown Error: XML Parser threw DOM exception at KVP");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
			try {
				Element kXML = doc.createElement("Key");
				kvp.appendChild(kXML);

				Text keyText = doc.createTextNode(key);
				kXML.appendChild(keyText);

			} catch (DOMException g) {
				KVMessage errorMessage = new KVMessage("resp",
						"Unknown Error: XML Parser threw DOM exception at key "
								+ key);
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
			String value = store.get(key);
			try {
				Element vXML = doc.createElement("Value");
				kvp.appendChild(vXML);
				Text vText = doc.createTextNode(value);
				vXML.appendChild(vText);
			} catch (DOMException h) {
				KVMessage errorMessage = new KVMessage("resp",
						"Unknown Error: XML Parser threw DOM exception at value "
								+ value);
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}

		TransformerFactory transfac = null;
		try {
			transfac = TransformerFactory.newInstance();
		} catch (TransformerFactoryConfigurationError e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: TransformerFactory threw exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Transformer trans = null;
		try {
			trans = transfac.newTransformer();
		} catch (TransformerConfigurationException e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: Transformer threw exception");
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
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: String converter threw exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}

		return xmlString;
	}

	public void dumpToFile(String fileName) throws KVException {
		String xml = toXML();// retrieve XML representation of KVStore
		FileWriter fw = null;
		try {
			fw = new FileWriter(fileName, false);// initialize FileWriter
		} catch (IOException e) {
			KVMessage message = new KVMessage("resp",
					"Unknown Error: file name invalid");
			throw new KVException(message);
		}
		try {
			fw.write(xml);// write out to file
		} catch (IOException e) {
			KVMessage message = new KVMessage("resp", "IO Error");
			throw new KVException(message);
		}
		try {
			fw.flush();// make sure files have been written
			fw.close();
		} catch (IOException e) {
			KVMessage message = new KVMessage("resp", "IO Error");
			throw new KVException(message);
		}
	}

	public void restoreFromFile(String fileName) throws KVException {
		// SO MUCH INITIALIZATION
		DocumentBuilderFactory fact;
		try {
			fact = DocumentBuilderFactory.newInstance();
		} catch (FactoryConfigurationError e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		DocumentBuilder docBuild;
		try {
			docBuild = fact.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw config exception");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		Document doc = null;
		File f;
		try {
			f = new File(fileName);// create file
		} catch (NullPointerException e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: file name was null");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		try {
			doc = docBuild.parse(f);// parse file
		} catch (SAXException e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: file is unparseable");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		} catch (IOException e) {
			KVMessage errorMessage = new KVMessage("resp", "IO Error");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		resetStore();// clear store
		NodeList nl = doc.getElementsByTagName("KVStore").item(0)
				.getChildNodes();// get list of all KVPairs
		for (int i = 0; i < nl.getLength(); i++) {// iterate through list
			Node kvp = nl.item(i);
			String key = kvp.getChildNodes().item(0).getFirstChild()
					.getNodeValue();// grab key
			String value = kvp.getChildNodes().item(1).getFirstChild()
					.getNodeValue();// grab value
			try {
				store.put(key, value);// put into KVStore's hashtable
			} catch (Exception e) {
				KVMessage errorMessage = new KVMessage("resp", "IO Error");
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
		}
	}

}
