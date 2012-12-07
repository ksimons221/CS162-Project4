/**
 * Slave Server component of a KeyValue store
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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class defines the slave key value servers. Each individual KVServer
 * would be a fully functioning Key-Value server. For Project 3, you would
 * implement this class. For Project 4, you will have a Master Key-Value server
 * and multiple of these slave Key-Value servers, each of them catering to a
 * different part of the key namespace.
 * 
 */
public class KVServer implements KeyValueInterface {
	private KVStore dataStore = null;
	private KVCache dataCache = null;
	ReentrantReadWriteLock.WriteLock kvStoreLock = null;
	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;

	/**
	 * @param numSets
	 *            number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);

		ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();
		kvStoreLock = masterLock.writeLock();

		AutoGrader.registerKVServer(dataStore, dataCache);
	}

	private KVException keyTooLarge() {
		KVMessage errorMessage = null;
		try {
			errorMessage = new KVMessage("resp", "Oversized key");
		} catch (KVException e) {
			return e;
		}
		return new KVException(errorMessage);
	}

	private KVException valueTooLarge() {
		KVMessage errorMessage = null;
		try {
			errorMessage = new KVMessage("resp", "Oversized value");
		} catch (KVException e) {
			return e;
		}
		return new KVException(errorMessage);
	}

	private boolean keyValidation(String key) {
		if (key == null) {
			return false;
		} else if (key.equals("") || key.length() == 0) {
			return false;
		} else {
			return true;
		}

	}

	public void verifyInput(String key, String value) throws KVException {
		if (keyValidation(key) == false) {
			throw badlyFormattedKey();
		}

		if (key.length() > MAX_KEY_SIZE) { // bigger then the max key size
			throw keyTooLarge();
		}

		if (value.length() > MAX_VAL_SIZE) { // bigger then the max value size
			throw valueTooLarge();
		}
	}

	private KVException badlyFormattedKey() {
		KVMessage errorMessage = null;
		try {
			errorMessage = new KVMessage("resp",
					"Unknown Error: The key is null or empty");
		} catch (KVException e) {
			return e;
		}
		return new KVException(errorMessage);
	}

	public void put(String key, String value) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerPutStarted(key, value);

		if (keyValidation(key) == false) {
			throw badlyFormattedKey();
		}

		if (key.length() > MAX_KEY_SIZE) { // bigger then the max key size
			throw keyTooLarge();
		}

		if (value.length() > MAX_VAL_SIZE) { // bigger then the max value size
			throw valueTooLarge();
		}

		dataCache.getWriteLock(key).lock();

		dataCache.put(key, value);

		kvStoreLock.lock();
		try {
			dataStore.put(key, value);
		} catch (KVException e) {
			AutoGrader.agKVServerPutFinished(key, value);
			kvStoreLock.unlock();
			dataCache.getWriteLock(key).unlock();
			throw e;
		}

		AutoGrader.agKVServerPutFinished(key, value);

		kvStoreLock.unlock();
		dataCache.getWriteLock(key).unlock();

		return;
	}

	public String get(String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerGetStarted(key);

		if (keyValidation(key) == false) {
			throw badlyFormattedKey();
		}

		dataCache.getWriteLock(key).lock();

		String cacheResult = dataCache.get(key);

		String storeResult = null;

		if (cacheResult == null) { // not in the cache

			kvStoreLock.lock();

			try {
				storeResult = dataStore.get(key);
			} catch (KVException e) { // / not in either
				AutoGrader.agKVServerGetFinished(key);
				kvStoreLock.unlock();
				dataCache.getWriteLock(key).unlock();
				throw e;
			}
			dataCache.put(key, storeResult);
			AutoGrader.agKVServerGetFinished(key);
			kvStoreLock.unlock();
			dataCache.getWriteLock(key).unlock();
			return storeResult;

		} else {
			AutoGrader.agKVServerGetFinished(key);
			dataCache.getWriteLock(key).unlock();
			return cacheResult;
		}
	}

	public void del(String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerDelStarted(key);

		if (keyValidation(key) == false) {
			throw badlyFormattedKey();
		}

		dataCache.getWriteLock(key).lock();

		dataCache.del(key);

		kvStoreLock.lock();

		try {
			dataStore.del(key);
		} catch (KVException e) {
			kvStoreLock.unlock();
			dataCache.getWriteLock(key).unlock();

			AutoGrader.agKVServerDelFinished(key);
			throw e;
		}

		kvStoreLock.unlock();
		dataCache.getWriteLock(key).unlock();

		AutoGrader.agKVServerDelFinished(key);
	}

	public KVCache getCache() {
		return dataCache;
	}

	public KVStore getStore() {
		return dataStore;
	}
}
