package edu.berkeley.cs162;

import junit.framework.TestCase;
import java.io.*;

import java.net.Socket;
import java.util.ArrayList;

import javax.xml.parsers.*;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;


public class KVMessageTest extends TestCase {
/**
 * KVMessage(String msgType) throws KVException

 */
	String putReqString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key>putKey</Key><Value>putValue</Value><TPCOpId>operationID</TPCOpId></KVMessage>";
	String getReqString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"getreq\"><Key>getKey</Key><TPCOpId>operationID</TPCOpId></KVMessage>";
	String delReqString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"delreq\"><Key>delKey</Key><TPCOpId>operationID</TPCOpId></KVMessage>";
	String unparseableReqString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"delreq\">delKey</Key><TPCOpId>operationID</TPCOpId></KVMessage>";
	String putReqString2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key></Key><Value>putValue</Value><TPCOpId>operationID</TPCOpId></KVMessage>";
	String putReqString3 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key>yourmomskey</Key><Value></Value><TPCOpId>operationID</TPCOpId></KVMessage>";
	String delReqString2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"delreq\"><Key></Key></KVMessage>";

	//NEW TO PROJ 4
	String readyString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"ready\"><TPCOpId>operationID</TPCOpId></KVMessage>";
	String abortString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"abort\"><Message>Error Message</Message><TPCOpId>operationID</TPCOpId></KVMessage>";
	String ackString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"ack\"><TPCOpId>operationID</TPCOpId></KVMessage>";
	String commitString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"commit\"><TPCOpId>operationID</TPCOpId></KVMessage>";
	
	String unparseableCommitString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"commit\"><TPCOpId></TPCOpId></KVMessage>";
	
	InputStream putReq;
	InputStream getReq;
	InputStream delReq;
	InputStream unparseableReq;
	InputStream putReq2;
	InputStream putReq3;
	InputStream delReq2;
	
	//NEW TO PROJ 4
	InputStream ready;
	InputStream abort;
	InputStream ack;
	InputStream commit;
	InputStream unparseableCommit;
	
	public void setUp()throws UnsupportedEncodingException{
		putReq = new ByteArrayInputStream(putReqString.getBytes("UTF-8"));
		getReq = new ByteArrayInputStream(getReqString.getBytes("UTF-8"));
		delReq = new ByteArrayInputStream(delReqString.getBytes("UTF-8"));
		unparseableReq = new ByteArrayInputStream(unparseableReqString.getBytes("UTF-8"));
		putReq2 = new ByteArrayInputStream(putReqString2.getBytes("UTF-8"));
		putReq3 = new ByteArrayInputStream(putReqString3.getBytes("UTF-8"));
		delReq2 = new ByteArrayInputStream(delReqString2.getBytes("UTF-8"));
		
		//NEW TO PROJ 4
		ready = new ByteArrayInputStream(readyString.getBytes("UTF-8"));
		abort = new ByteArrayInputStream(abortString.getBytes("UTF-8"));
		ack = new ByteArrayInputStream(ackString.getBytes("UTF-8"));
		commit = new ByteArrayInputStream(commitString.getBytes("UTF-8"));
		unparseableCommit = new ByteArrayInputStream(unparseableCommitString.getBytes("UTF-8"));
	}
	
	public void testConstructor1() throws KVException {
		KVMessage message = new KVMessage("getreq");
		assertEquals(message.getMsgType(), "getreq");
		message = new KVMessage("putreq");
		assertEquals(message.getMsgType(), "putreq");
		boolean thrown = false;
		try{
			message = new KVMessage("yourmomreq");
		}
		catch (KVException e){
			if (e.getMsg().getMessage().equals("Unknown Error: msgType invalid")){
			thrown = true;
			}
		}
		assertTrue(thrown);
		}
	
	
	
	public void testConstructor2() throws KVException{
		String message1 = "oh say can you see";
		String message2 = "by the dawn's early light";
		boolean thrown= false;
		KVMessage message = new KVMessage("putreq", message1);
		assertEquals(message.getMessage(), message1);
		assertEquals(message.getMsgType(), "putreq");
		try{
			message = new KVMessage("yourmomreq", message2);
		}
		catch (KVException e){
			if (e.getMsg().getMessage().equals("Unknown Error: msgType invalid")){
			thrown = true;
			}
		}
		assertTrue(thrown);
	}
	
	public void testConstructor3() throws KVException, UnsupportedEncodingException{
		//test a put
		KVMessage putMessage = new KVMessage(putReq);
		assertEquals(putMessage.getMsgType(), "putreq");
		assertEquals(putMessage.getKey(), "putKey");
		assertEquals(putMessage.getValue(), "putValue");
		assertEquals(putMessage.getTpcOpId(), "operationID");
		
		//test a get
		KVMessage getMessage = new KVMessage(getReq);
		assertEquals(getMessage.getMsgType(), "getreq");
		assertEquals(getMessage.getKey(), "getKey");
		assertEquals(getMessage.getTpcOpId(), "operationID");
		
		
		//test a del
		KVMessage delMessage = new KVMessage(delReq);
		assertEquals(delMessage.getMsgType(), "delreq");
		assertEquals(delMessage.getKey(), "delKey");
		assertEquals(delMessage.getTpcOpId(), "operationID");
		
		//test an unparseable inputstream
		boolean thrown = false;
		try {
			new KVMessage(unparseableReq);
		}
		catch (KVException e) {
			assertEquals(e.getMsg().getMsgType(), "resp");
			thrown = true;	
		}
		assertTrue(thrown);
		thrown=false;
		//try a put with no key
		try {
			new KVMessage(putReq2);
		}
		catch (KVException e){
			assertEquals(e.getMsg().getMsgType(), "resp");
			thrown=true;
		}
		assertTrue(thrown);
		thrown=false;
		//try a put with no value 
		try {
			new KVMessage(putReq3);
		}
		catch (KVException e){
			assertEquals(e.getMsg().getMsgType(), "resp");
			thrown=true;
		}
		assertTrue(thrown);
		thrown=false;
		//del with no key
		try {
			new KVMessage(delReq2);
		
		}
		catch (KVException e){
			assertEquals(e.getMsg().getMsgType(), "resp");
			thrown=true;
		}
		assertTrue(thrown);
	}
	

	public void testToXML() throws KVException{
		
		KVMessage putMessage = new KVMessage(putReq);
		assertEquals(putMessage.toXML(), putReqString);
		
		KVMessage getMessage = new KVMessage(getReq);
		assertEquals(getMessage.toXML(), getReqString);

		KVMessage delMessage = new KVMessage(delReq);
		assertEquals(delMessage.toXML(), delReqString);

	}

	public void testSendMessage() {
		//IMPLEMENT ME 
	}
	
	//tests new msgtypes: ack, ready, commit, abort, registration
	public void testConstructor4() throws KVException{
		
		//test a ready
		KVMessage readyMessage = new KVMessage(ready);
		assertEquals(readyMessage.getMsgType(), "ready");
		assertEquals(readyMessage.getTpcOpId(), "operationID");
		//test an abort
		KVMessage abortMessage = new KVMessage(abort);
		assertEquals(abortMessage.getMsgType(), "abort");
		assertEquals(abortMessage.getTpcOpId(), "operationID");
		assertEquals(abortMessage.getMessage(), "Error Message");
		//test an ack
		KVMessage ackMessage = new KVMessage(ack);
		assertEquals(ackMessage.getMsgType(), "ack");
		assertEquals(ackMessage.getTpcOpId(), "operationID");
		//test a commit
		KVMessage commitMessage = new KVMessage(commit);
		assertEquals(commitMessage.getMsgType(), "commit");
		assertEquals(commitMessage.getTpcOpId(), "operationID");
		//test a commit without operation ID
		boolean kvThrown=false;
		try{
			new KVMessage(unparseableCommit);
		}
		catch (KVException e){
			kvThrown=true;
		}
		assertTrue(kvThrown);
	}

}
