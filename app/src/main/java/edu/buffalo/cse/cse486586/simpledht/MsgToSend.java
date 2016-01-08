package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pragya on 3/31/15.
 */
public class MsgToSend implements Serializable {


    public String msgType;
    public String senderPort;
    public String predes;
    public String successor;
    public String nodeId;
    public String msgKey;
    public String msgValue;
    public String receiverPort;
    public HashMap<String,String> message =new HashMap<String,String>();



    public HashMap<String, String> getMessage() {
        return message;
    }

    public void setMessage(HashMap<String, String> message) {
        this.message = message;
    }



    public MsgToSend() {
      //  super();
    }

    public MsgToSend(String msgType, String senderPort, String predes, String successor, String nodeId, String msgKey, String msgValue, String receiverPort,HashMap<String,String> message) {

        this.msgType = msgType;
        this.senderPort = senderPort;
        this.predes = predes;
        this.successor = successor;
        this.nodeId = nodeId;
        this.msgKey =msgKey;
        this.msgValue = msgValue;
        this.receiverPort =receiverPort;
        this.message =message;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

    public String getPredes() {
        return predes;
    }

    public void setPredes(String predes) {
        this.predes = predes;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

}
