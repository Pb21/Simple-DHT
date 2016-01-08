package edu.buffalo.cse.cse486586.simpledht;

import java.util.ArrayList;

/**
 * Created by pragya on 4/1/15.
 */
public class Tracker {


    private String predecessor;
    private String successor;
    private String nodeId;
    private String actualPortNo;
    private ArrayList<String> ownNodeList;

    public ArrayList<String> getOwnNodeList() {
        return ownNodeList;
    }

    public void setOwnNodeList(ArrayList<String> ownNodeList) {
        this.ownNodeList = ownNodeList;
    }

    public String getActualPortNo() {
        return actualPortNo;
    }

    public void setActualPortNo(String actualPortNo) {
        this.actualPortNo = actualPortNo;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
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
