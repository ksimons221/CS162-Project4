package edu.berkeley.cs162;

import junit.framework.TestCase;
import java.util.ArrayList;
import java.io.File;

public class TPCLogTest extends TestCase {
KVServer kvs;
TPCLog log;
ArrayList<KVMessage> list;
String logPath = "1@stuff";
	protected void setUp() throws Exception {
		list = new ArrayList<KVMessage>();
		KVMessage msg = new KVMessage("putreq");
		msg.setKey("1");
		msg.setValue("1");
		msg.setTpcOpId("1");
		list.add(msg);

		KVMessage resp = new KVMessage("commit");
		resp.setTpcOpId("1");
		list.add(resp);

		KVMessage msg2 = new KVMessage("putreq");
		msg2.setKey("2");
		msg2.setValue("2");
		msg2.setTpcOpId("2");
		list.add(msg2);

		KVMessage resp2 = new KVMessage("commit");
		resp2.setTpcOpId("2");
		list.add(resp2);

		KVMessage msg3 = new KVMessage("putreq");
		msg3.setKey("3");
		msg3.setValue("3");
		msg3.setTpcOpId("3");
		list.add(msg3);

		KVMessage resp3 = new KVMessage("commit");
		resp3.setTpcOpId("3");
		list.add(resp3);

		KVMessage msg4 = new KVMessage("putreq");
		msg4.setKey("4");
		msg4.setValue("4");
		msg4.setTpcOpId("4");
		list.add(msg4);

		KVMessage resp4 = new KVMessage("commit");
		resp4.setTpcOpId("4");
		list.add(resp4);

		KVMessage msg5 = new KVMessage("delreq");
		msg5.setKey("3");
		msg5.setTpcOpId("5");
		list.add(msg5);

		KVMessage resp5 = new KVMessage("commit");
		resp5.setTpcOpId("5");
		list.add(resp5);
		
		
		KVMessage msg56 = new KVMessage("ignoreNext");
		list.add(msg56);
		
		
		KVMessage msg6 = new KVMessage("putreq");
		msg6.setKey("1");
		msg6.setValue("10");
		msg6.setTpcOpId("1");
		list.add(msg6);

		KVMessage resp6 = new KVMessage("commit");
		resp6.setTpcOpId("1");
		list.add(resp6);
		
		
		KVMessage msg7 = new KVMessage("ignoreNext");
		list.add(msg7);
		
		this.kvs = new KVServer(100,100);
		this.log = new TPCLog(logPath,kvs);
	}
	
	

	public void testAppendAndFlush() throws KVException{
		for (KVMessage message : list) {
			log.appendAndFlush(message);
		}
		log.rebuildKeyServer();

		boolean throwsException=false;
		assertEquals(this.kvs.get("1"), "1");
		assertEquals(this.kvs.get("2"), "2");
		assertEquals(true, log.ignoreNext);
		
		try{
			assertEquals(this.kvs.get("3"), "3");
		}
		catch(KVException e){
			throwsException=true;
			assertEquals(e.getMsg().getMessage(), "Does not exist");
		}
		assertEquals(this.kvs.get("4"), "4");
		assertTrue(throwsException);
	}
	

}
