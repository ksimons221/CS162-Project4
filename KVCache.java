/**
 * Implementation of a set-associative cache.
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import java.util.ArrayList;

import javax.xml.parsers.*;

import java.io.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET). If a set is
 * full and another entry is added, an entry is dropped based on the eviction
 * policy.
 */
public class KVCache implements KeyValueInterface {
	private int numSets = 100;
	private int maxElemsPerSet = 10;
	// QUESTION 5
	private LinkedList[] sets;
	private final int KEY = 0;
	private final int VALUE = 1;
	private final int USED = 2;
	HashMap<Integer, ReentrantReadWriteLock.WriteLock> writeLocks = new HashMap<Integer, ReentrantReadWriteLock.WriteLock>();

	/**
	 * Creates a new LRU cache.
	 * 
	 * @param cacheSize
	 *            the maximum number of entries that will be kept in this cache.
	 */
	public KVCache(int numSets, int maxElemsPerSet) {
		this.numSets = numSets;
		this.maxElemsPerSet = maxElemsPerSet;
		this.sets = new LinkedList[numSets];
		for (int i = 0; i < this.numSets; i++) {
			ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
			ReentrantReadWriteLock.WriteLock masterWriteLock = masterLock
					.writeLock();

			writeLocks.put(i, masterWriteLock);
			this.sets[i] = new LinkedList<String[]>();
		}
	}

	/**
	 * Retrieves an entry from the cache. Assumes the corresponding set has
	 * already been locked for writing.
	 * 
	 * @param key
	 *            the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this
	 *         key exists in the cache.
	 */
	public String get(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheGetDelay();

		LinkedList<String[]> currentSet = sets[getSetId(key)];

		for (int i = 0; i < currentSet.size(); i++) {

			String[] currentTuple = currentSet.get(i);

			if (currentTuple[this.KEY].equals(key)) {

				currentTuple[this.USED] = "true";

				AutoGrader.agCacheGetFinished(key);

				return currentTuple[this.VALUE];

			}

		}
		AutoGrader.agCacheGetFinished(key);

		return null;
	}

	/**
	 * Adds an entry to this cache. If an entry with the specified key already
	 * exists in the cache, it is replaced by the new entry. If the cache is
	 * full, an entry is removed from the cache based on the eviction policy
	 * Assumes the corresponding set has already been locked for writing.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated.
	 * @param value
	 *            a value to be associated with the specified key.
	 * @return true is something has been overwritten
	 */
	public void put(String key, String value) {
		// Must be called before anything else
		AutoGrader.agCachePutStarted(key, value);
		AutoGrader.agCachePutDelay();

		LinkedList<String[]> currentSet = sets[getSetId(key)];

		for (int i = 0; i < currentSet.size(); i++) {

			String[] currentTuple = currentSet.get(i);

			if (currentTuple[this.KEY].equals(key)) {

				currentTuple[this.USED] = "true";

				currentTuple[this.VALUE] = value;

				AutoGrader.agCachePutFinished(key, value);

				return;

			}

		}

		// // Else not in the cache queue

		String[] newValue = new String[3];
		newValue[KEY] = key;
		newValue[VALUE] = value;
		newValue[USED] = "false";

		if (currentSet.size() == maxElemsPerSet) {

			int counter = 0;
			String[] currentTuple;

			while (true) {

				currentTuple = currentSet.get(counter);

				if (currentTuple[USED].equals("false")) { // evict this bitch.
															// WE FOUND IT

					currentSet.remove(counter);

					break;

				} else { // / SET TO ZERO AND PUT TO THE END//SET USED BIT

					currentTuple = currentSet.remove(counter);

					currentTuple[USED] = "false";

					currentSet.add(currentTuple);

				}

				counter++;

				counter = counter % maxElemsPerSet;
			}
		}

		currentSet.add(newValue);

		// Must be called before returning
		AutoGrader.agCachePutFinished(key, value);

		return;
	}

	/**
	 * Removes an entry from this cache. Assumes the corresponding set has
	 * already been locked for writing.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated.
	 */
	public void del(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheDelDelay();

		LinkedList<String[]> currentSet = sets[getSetId(key)];

		for (int i = 0; i < currentSet.size(); i++) {

			String[] currentTuple = currentSet.get(i);

			if (currentTuple[this.KEY].equals(key)) {

				currentSet.remove(i);

			}

		}

		// Must be called before returning
		AutoGrader.agCacheDelFinished(key);

	}

	/**
	 * @param key
	 * @return the write lock of the set that contains key.
	 */
	public WriteLock getWriteLock(String key) {
		return writeLocks.get(getSetId(key));
	}

	/**
	 * 
	 * @param key
	 * @return set of the key
	 */
	private int getSetId(String key) {
		return (key.hashCode() & 0x7FFFFFFF) % numSets;
	}

	// //// USED FOR TESTING IN JUNIT
	public int getNumSets() {
		return this.numSets;
	}

	public int getMaxElemsPerSet() {
		return this.maxElemsPerSet;
	}

	public LinkedList[] getSets() {
		return this.sets;
	}

	public HashMap getWriteLocks() {
		return this.writeLocks;
	}

	public String usedBit(String key) {
		int setID = getSetId(key);
		LinkedList<String[]> currentSet = this.sets[setID];
		for (int i = 0; i < this.sets[setID].size(); i++) {
			if (currentSet.get(i)[KEY] == key) {
				return currentSet.get(i)[USED];
			}
		}
		return "Error, key not found";

	}

	public String toXML() throws KVException {
		// INITIALIZE DOCUMENT/BUILDER/FACTORY
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
		// REAL XML STARTS HERE
		Element kvc = null;
		try {
			kvc = doc.createElement("KVCache");// create kvcache element
			doc.appendChild(kvc);
		} catch (DOMException e) {
			KVMessage errorMessage = new KVMessage("resp",
					"Unknown Error: XML Parser threw DOM exception at type");
			KVException toThrow = new KVException(errorMessage);
			throw toThrow;
		}
		// ITERATE THROUGH SETS
		for (int i = 0; i < this.numSets; i++) {
			LinkedList<String[]> currentSet = sets[i];
			Element setXML = null;// create set element
			try {
				setXML = doc.createElement("Set");
				setXML.setAttribute("Id", new Integer(i).toString()); // set Id
																		// attribute
				kvc.appendChild(setXML);

			} catch (DOMException e) {
				KVMessage errorMessage = new KVMessage("resp",
						"Unknown Error: XML Parser threw DOM exception at set "
								+ i);
				KVException toThrow = new KVException(errorMessage);
				throw toThrow;
			}
			// ITERATE THROUGH CACHES IN SET
			for (int j = 0; j < this.maxElemsPerSet; j++) {
				int currentKey = j;
				if (j < currentSet.size()) { // check if cache is valid
					String[] currentEntry = currentSet.get(currentKey);
					Element cacheXML = null;
					try {
						cacheXML = doc.createElement("CacheEntry");// create
																	// CacheEntry
																	// element
						cacheXML.setAttribute("isReferenced",
								currentEntry[USED]);
						cacheXML.setAttribute("isValid", "true");// set
																	// attributes
						setXML.appendChild(cacheXML);

					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at CacheEntry "
										+ currentKey);
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
					String key = currentEntry[KEY];// retrieve key
					try {
						Element kXML = doc.createElement("Key");// create key
																// element
						cacheXML.appendChild(kXML);
						Text kText = doc.createTextNode(key);// add key data
						kXML.appendChild(kText);
					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at key "
										+ key);
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
					String value = currentEntry[VALUE];// retrieve value
					try {
						Element vXML = doc.createElement("Value");// create
																	// value
																	// element
						cacheXML.appendChild(vXML);
						Text vText = doc.createTextNode(value);// add value data
						vXML.appendChild(vText);
					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at value "
										+ value);
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
				} else {// add invalid caches
					Element cacheXML = null;
					try {
						cacheXML = doc.createElement("CacheEntry");
						cacheXML.setAttribute("isReferenced", "false");
						cacheXML.setAttribute("isValid", "false");
						setXML.appendChild(cacheXML);

					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at invalid CacheEntry");
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
					// both key and value will be just tags
					// do we need empty data values?
					try {
						Element kXML = doc.createElement("Key");
						cacheXML.appendChild(kXML);
						Text kText = doc.createTextNode("");// add value data
						kXML.appendChild(kText);
					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at key in invalid CacheEntry");
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
					try {
						Element vXML = doc.createElement("Value");
						cacheXML.appendChild(vXML);
						Text vText = doc.createTextNode("");// add value data
						vXML.appendChild(vText);
					} catch (DOMException e) {
						KVMessage errorMessage = new KVMessage("resp",
								"Unknown Error: XML Parser threw DOM exception at value in invalid CacheEntry");
						KVException toThrow = new KVException(errorMessage);
						throw toThrow;
					}
				}
			}
		}
		// CONVERT TO STRING
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

	@Override
	public String toString() {
		String toReturn = "";
		String newLine = System.getProperty("line.separator");
		for (int j = 0; j < this.numSets; j++) {
			LinkedList<String[]> currentSet = this.sets[j];
			toReturn += (newLine + "SET " + j + " :");
			for (int i = 0; i < currentSet.size(); i++) {
				toReturn += (currentSet.get(i)[VALUE] + " ");
			}
		}
		return toReturn;
	}

}
