package edu.berkeley.cs162;

import junit.framework.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class KVCacheTest extends TestCase {
	private final int KEY = 0;
	private final int VALUE = 1;
	private final int USED = 2;
	KVCache testCache;
	int numSets;
	int setLength;
	
	String string0= "0";	//hashes to set 0
	String string1= "1";	//hashes to set 1
	String string2= "4";	//hashes to set 0
	String string3= "5";	//hashes to set 1
	String string4 = "8";	//hashes to set 0 
	String string5 = "9";	//hashes to set 1 

	
	
	
	
	public void setUp(){
		//test Cache has 4 sets with max 2 elements per set
		numSets=4;
		setLength=2;
		testCache= new KVCache(numSets, setLength);
	}
	
	public void testKVCache() {
		assertEquals(testCache.getNumSets(), numSets);
		assertEquals(testCache.getMaxElemsPerSet(), setLength);
		LinkedList[] sets= testCache.getSets();
		assertEquals(sets.length, numSets);
		for (int i = 0; i < setLength; i++){
			assertTrue(testCache.getWriteLocks().containsKey(i));
		}	
		
	}

	public void testGet() {
		//test with key we don't have
		assertNull(testCache.get("key"));
		assertNull(testCache.get("key1"));
	}
	

	public void testPut() {
		//test that we can put up to setLength things in a set
		//0 hashes to 48=set 0
		//1 hashes to 49= set 1 
		String[] values = {"do", "re", "mi", "fa"};
		//test case where cache has room

		//test case where cache has room
		//put "do" and "re" into set 0 
		
		testCache.put(string0, "do");
		
		//test case where same value is in cache
		testCache.put(string0, "do");
		assertEquals(testCache.usedBit(string0), "true");
		
		
		assertEquals(testCache.get(string0), "do");
		
		testCache.put(string2, "re");
		assertEquals(testCache.get(string2), "re");
		
		//test case where same value is in cache
		testCache.put(string0, "do");
		
		assertEquals(testCache.get(string0), "do");
		
		//test case where different value is in cache
		testCache.put(string0, "mi");
		assertEquals(testCache.get(string0), "mi");

		//test case with replacement
		//want "mi" to get evicted
		testCache.put(string4, "fa");
		assertEquals(testCache.get(string4), "fa");
		assertFalse(testCache.getSets()[0].contains("mi"));
		testCache.put(string0, "so");
		//want to evict "re"
		assertFalse(testCache.getSets()[0].contains("re"));

		}

	

	
	
	public void testDel() {
		testCache= new KVCache(numSets, setLength+2);
		testCache.put(string0, "do");
		testCache.put(string2, "re");
		testCache.put(string4, "mi");
		assertEquals(testCache.getSets()[0].size(), 3);
		testCache.del(string2);
		assertEquals(testCache.getSets()[0].size(), 2);
	}


	public void testPar1(){
		//across different sets
		testCache= new KVCache(numSets, setLength);
		Runnable r1 = new Runnable(){
			  public void run() {
				   testCache.put(string0, "do");
				  }	
		};
		Runnable r2 = new Runnable(){
			  public void run() {
				   testCache.put(string1, "do");
				  }	
		};
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		t1.start();
		t2.start();
		while (t1.isAlive() || t2.isAlive()){
			//hang out
		}
		assertEquals(testCache.getSets()[0].size(), 1);
		assertEquals(testCache.getSets()[1].size(), 1);

		
	}
	
	
	public void testPar2(){
		//in same set
		testCache= new KVCache(numSets, setLength);
		Runnable r1 = new Runnable(){
			  public void run() {
				   testCache.put(string0, "do");
				  }	
		};
		Runnable r2 = new Runnable(){
			  public void run() {
				   testCache.put(string2, "re");
				  }	
		};
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		t1.start();
		t2.start();
		while (t1.isAlive() || t2.isAlive()){
			//hang out
		}
		assertEquals(testCache.getSets()[0].size(), 2);
		
	}
	
	public void testPar3(){
		//in same set with same key 
		testCache= new KVCache(numSets, setLength);
		Runnable r1 = new Runnable(){
			  public void run() {
				   testCache.put(string0, "do");
				   testCache.put(string2, "fa");
				  }	
		};
		Runnable r2 = new Runnable(){
			  public void run() {
				   testCache.put(string0, "re");
				  }	
		};
		Thread t1 = new Thread(r1);
		Thread t2 = new Thread(r2);
		t1.start();
		t2.start();
		while (t1.isAlive() || t2.isAlive()){
			//hang out
		}
		assertEquals(testCache.getSets()[0].size(), 2);		
	}
		
		
	public void testToXML() throws KVException{
		testCache= new KVCache(2, 2);
		testCache.put("0", "oh");
		testCache.put("2", "say");
		testCache.put("1", "can");
		testCache.put("3", "you");
		String shouldBe="<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVCache><Set Id=\"0\"><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>0</Key><Value>oh</Value></CacheEntry><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>2</Key><Value>say</Value></CacheEntry></Set><Set Id=\"1\"><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>1</Key><Value>can</Value></CacheEntry><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>3</Key><Value>you</Value></CacheEntry></Set></KVCache>";
		assertEquals(testCache.toXML(), shouldBe);
		testCache.get("3");
		shouldBe="<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVCache><Set Id=\"0\"><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>0</Key><Value>oh</Value></CacheEntry><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>2</Key><Value>say</Value></CacheEntry></Set><Set Id=\"1\"><CacheEntry isReferenced=\"false\" isValid=\"true\"><Key>1</Key><Value>can</Value></CacheEntry><CacheEntry isReferenced=\"true\" isValid=\"true\"><Key>3</Key><Value>you</Value></CacheEntry></Set></KVCache>";
		assertEquals(testCache.toXML(), shouldBe);
			
		}
	

}
