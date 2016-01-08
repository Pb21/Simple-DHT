package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String[] PORTS = {"5554", "5556", "5558", "5560", "5562"};
    static final int SERVER_PORT = 10000;
    String portStr;
    static final String RING_REQ="Form Ring";
    static final String RING_ACK="Ring formed";
    static final String INSERT_DATA ="Insert data";
    static final String IN_NODE ="Insert data in proper node";
    static final String STAR_Q = "Star query from all nodes";
    static final String DONE = "DONE";

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private static boolean TRAVEL_FULL_RING = false;
    private static boolean TRAVEL_FULL_RING_DELETE = false;
    private static boolean RING_FORMED = false;
    private static boolean valueNotReturned = false;
    private static boolean valueNotReturneddelete = false;

    private static final String SEARCH_KEY = "search for a key in the ring";
    private static final String SEARCH_KEY_TO_DELETE = "search for a key to delete in the ring";
    private static final String SEARCH_KEY_TO_DELETE_ANS = "key found to delete to delete in the ring";
    private static final String DELETED_SINGLE_KEY = "key deleted response to the sender node in the ring";
    private static final String DELETE_REQ = "delete in the ring";
    private static final String SEARCH_KEY_ANS = "Key found in the ring";


    Map<String,String> totalRingNodes = new HashMap<String,String>(5);
    ArrayList<String> listOfNodes = new ArrayList<String>();

    Map<String,String> totalRingNodesTemp = new HashMap<String,String>(5);
    HashMap <String,String> wholeResult = new HashMap<String,String>();
    /*keeps a track of the predes and succ for each node*/
    private Tracker trck = new Tracker();
    private SQLiteDatabase db;

    private String resultValue=null;
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equalsIgnoreCase("\"*\""))
        {
            if(RING_FORMED){

                Log.d("STAR DELETE Query ", "Ring formed deleting from port "+portStr);

                String suc= totalRingNodesTemp.get(trck.getSuccessor());
                Log.d("Query function", "Successor if "+suc);
                MsgToSend mq=new MsgToSend(DELETE_REQ,portStr,null,null,null,null,null,suc,null);
                try {
                    helperFunction(mq);
                }catch(UnknownHostException e){
                    Log.e("Error","its a * query UnKnownHosterror "+e);
                }catch(IOException e){
                    Log.e("Error","its a * query IOExceptionerror "+e);
                }

                Log.d("STAR DELETE query","Line 211 waiting for full result");
                /*Waiting for all the responses from the avds*/
                TRAVEL_FULL_RING_DELETE = false;
                while(!TRAVEL_FULL_RING_DELETE){}

                return 0;

            }else{
                Log.d("* DELETE query","ONLY 1 node is present");

                db.delete(DataBaseConnector.TABLE_NAME, null,null);
                return 0;
            }
        }  else if(selection.equalsIgnoreCase("\"@\"")){

            Log.d("Selection : @", "@ : NO RING");

            int p = db.delete(DataBaseConnector.TABLE_NAME, null,null);
            Log.d("@ DELETE testing", "@ testing, count "+p);
            return 0;
        }
        else{
            if(!RING_FORMED) {

                Log.d("single testing", "key is " + selection + " port is " + portStr);

                String whereclause = DataBaseConnector.TABLE_COLUMN1 + " = '" + selection + "'";
                int c = db.delete(DataBaseConnector.TABLE_NAME, whereclause, null);

                Log.d("single DELETE testing", "single testing, count "+c);
                return 0;
            }else{
                Log.d("single DELETE testing", "single DELETE testing, with RING for key "+selection+" started from port "+portStr);

                valueNotReturneddelete=false;
                MsgToSend m1 = new MsgToSend(SEARCH_KEY_TO_DELETE, portStr, null, null, null, selection,null,null,null);
                try {
                    helperFunction(m1);
                } catch (UnknownHostException e) {
                    Log.e("error", "from helper function inside insert" + e);
                } catch (IOException e) {
                    Log.e("error", "from helper function inside insert" + e);
                }

                Log.d("valueNotReturned is","v");

                while(!valueNotReturneddelete){}

                Log.d("Back after deleting everything", "Back");
                return 0;
            }
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String keys = values.getAsString("key");
        String value = values.getAsString("value");

        if (RING_FORMED) {

            Log.d("Insert function","Ring formed is true");

            String myNode = trck.getNodeId();
                // public MsgToSend(String msgType, String senderPort, String predes, String successor, String nodeId, String msgKey, String msgValue) {
                MsgToSend messg = new MsgToSend(INSERT_DATA, portStr, null, null, myNode, keys, value,null,null);
                try {
                    helperFunction(messg);
                } catch (UnknownHostException e) {
                    Log.e("error", "from helper function inside insert" + e);
                } catch (IOException e) {
                    Log.e("error", "from helper function inside insert" + e);
                }

        }else{
                /*Ring is not yet formed and it is the only AVD running so far*/
               Log.d("insert function","Ring not yet formed,hence data inserted in the only avd");
               databaseInsert(keys,value);
            }
        return uri;
    }


    /*Inserts the data in the database for the proper node*/
    public void databaseInsert(String key, String value){

        ContentValues cv = new ContentValues();
        cv.put(KEY_FIELD,key);
        cv.put(VALUE_FIELD,value);

        long rId=db.insert(DataBaseConnector.TABLE_NAME,"",cv);
        if (rId>0) {
            Log.v("insert", cv.toString());
        }
        if(!RING_FORMED)
            Log.d("database Insert","Ring not formed inserted in the database key   "+key+ "port "+portStr);
        else
            Log.d("database Insert","RING inserted in the database key   "+key+ "port "+portStr);
    }

    public boolean createDb() {
        // If you need to perform any one-time initialization task, please do it here.
        Context context=getContext();
        DataBaseConnector dbc=new DataBaseConnector(context);

        db=dbc.getWritableDatabase();
        if(db==null) {
            Log.d("Failure","Couldnt connect to database");
            return false;
        }
        else {
            dbc.onUpgrade(db,1,1);
            Log.d("Success","Connection established successfully"+db);
            return true;
        }
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        Log.d("Inside onCreate","We are started");

        //creating the database for each emulator
        createDb();
        populateHashedPorts();
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,portStr);

        } catch (IOException e) {
           Log.e("Error", "Can't create a ServerSocket");
        }

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        Log.d("inside query function", "Starting to query");

        MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});
        if(selection.equalsIgnoreCase("\"*\""))
        {
            if(RING_FORMED){
                wholeResult = new HashMap<String,String>();
                Log.d("STAR Query ", "Ring formed.Querying from port "+portStr);

                String suc= totalRingNodesTemp.get(trck.getSuccessor());
                Log.d("Query function", "Successor if "+suc);

                MsgToSend mq=new MsgToSend(STAR_Q,portStr,null,null,null,null,null,suc,null);

                try {
                    helperFunction(mq);
                }catch(UnknownHostException e){
                    Log.e("Error","its a * query UnKnownHosterror "+e);
                }catch(IOException e){
                    Log.e("Error","its a * query IOExceptionerror "+e);
                }

                Log.d("STAR query","Line 211 waiting for full result");
                /*Waiting for all the responses from the avds*/
                while(!TRAVEL_FULL_RING){}

                if(!wholeResult.isEmpty()) {

                    Log.d("STAR query","whole result is not empty");

                    Set set = wholeResult.entrySet();
                    Iterator i = set.iterator();

                    while (i.hasNext()) {
                        Map.Entry me = (Map.Entry) i.next();
                        String[] eachRow = new String[2];
                        eachRow[0] = (String) me.getKey();
                        eachRow[1] = (String) me.getValue();
                        mc.addRow(eachRow);
                    }
                }
            return mc;

        }else{
                Log.d("* query","ONLY 1 node is present");

                String query = "SELECT  * FROM " + DataBaseConnector.TABLE_NAME;
                Cursor cursor = db.rawQuery(query, null);
                int keyIndex = cursor.getColumnIndex(KEY_FIELD);
                int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

                if (cursor.moveToFirst()) {
                    do {
                        String returnKey = cursor.getString(keyIndex);
                        String returnValue = cursor.getString(valueIndex);
                        } while (cursor.moveToNext());
                }

//            cursor.moveToFirst();
                return cursor;

            }
        }
        else if(selection.equalsIgnoreCase("\"@\"")){


                Log.d("Selection : @", "@ : NO RING");
                String query = "SELECT  * FROM " + DataBaseConnector.TABLE_NAME;
                Cursor cursor = db.rawQuery(query, null);

                int keyIndex = cursor.getColumnIndex(KEY_FIELD);
                int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

                if (cursor.moveToFirst()) {
                    do {
                        String returnKey = cursor.getString(keyIndex);
                        String returnValue = cursor.getString(valueIndex);

                        Log.d("Printing the values", "key---------" + returnKey);
                        Log.d("Printing the values", "value-------" + returnValue);
                    } while (cursor.moveToNext());
                }
                //cursor.moveToFirst()


        return cursor;
                //cursor.close();
        }else{
                if(!RING_FORMED) {

                    Log.d("single testing", "key is " + selection + " port is " + portStr);

                    String whereclause = DataBaseConnector.TABLE_COLUMN1 + " = '" + selection + "'";
                    Cursor cursor = db.query(DataBaseConnector.TABLE_NAME, null, whereclause, null, null, null, null);

                    int keyIndex = cursor.getColumnIndex(KEY_FIELD);
                    int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

                    if (cursor.moveToFirst()) {

                        do {
                            String returnKey = cursor.getString(keyIndex);
                            String returnValue = cursor.getString(valueIndex);

                            Log.d("Printing the values", "key---------" + returnKey);
                            Log.d("Printing the values", "value-------" + returnValue);
                        } while (cursor.moveToNext());
                    }

                    cursor.moveToFirst();
                    return cursor;
                }else{

                    wholeResult=new HashMap<String,String>();
                    valueNotReturned=false;
                    MsgToSend m1 = new MsgToSend(SEARCH_KEY, portStr, null, null, null, selection,null,null,null);
                    try {

                        helperFunction(m1);
                        Log.d("lddis","v");
                    } catch (UnknownHostException e) {
                        Log.e("error", "from helper function inside insert" + e);
                    } catch (IOException e) {
                        Log.e("error", "from helper function inside insert" + e);
                    }

                    Log.d("valueNotReturned is","v");
                    while(!valueNotReturned){}

                    Log.d("Back", "Back");
                    String[] eachRow = new String[2];
                    eachRow[0]=selection;
                    eachRow[1]=wholeResult.get(selection);
                    mc.addRow(eachRow);
                    return mc;

                }
        }
       // return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            Log.d("@@SERVER started@@@",portStr);
            if(portStr.equalsIgnoreCase("5554")){
                Log.d("***Master***","I am 5554");

            }
            try {
                while (true) {
                    ServerSocket serverSocket = (ServerSocket) sockets[0];
                    Socket sSocket = serverSocket.accept();

                    ObjectInputStream ois =
                            new ObjectInputStream(sSocket.getInputStream());

                    MsgToSend msgRecvd = (MsgToSend) ois.readObject();

                    Log.d("At server", "Received message from  " + msgRecvd.senderPort+ " Msg type  "+msgRecvd.msgType);
                    //msgRecvd is the message transmitted from the emulator on startup
                    if (portStr.equals("5554") && msgRecvd.msgType.equals(RING_REQ)) {

                        String nodeId;
                        Log.d("At server", "I am 5554 and request to form ring");
                        /**If the sender is 5554, it is the first node in the ring, and hence it will be its own predes and successor**/
                        if (msgRecvd.senderPort.equals("5554") && !RING_FORMED) {

                            try {
                                nodeId = genHash(msgRecvd.senderPort);
                                /*Keeping a track of the node Ids present in the ring*/

                                Log.d("At server", "Line 169 inside 5554 nodeId****");

                                listOfNodes.add(nodeId);
                                trck.setNodeId(nodeId);
                                trck.setSuccessor(nodeId);
                                trck.setPredecessor(nodeId);
                            } catch (NoSuchAlgorithmException e) {
                                Log.d("FATAL error", e.getMessage());
                            }
                        } else {
                            /**Joining the other(other than 5554) nodes**/
                            RING_FORMED = true;
                            try {
                                listOfNodes.add(genHash(msgRecvd.senderPort));

                            } catch (NoSuchAlgorithmException e) {
                                Log.d("FATAL error", e.getMessage());
                            }

                            Log.d("At server", "I am just an ordinary node. listOfNodes.size is " + listOfNodes.size());
                            //sort on basis of nodeIds to form ring
                            Collections.sort(listOfNodes);

                            //figuring out the predes and succ,creating the ring
                            for (int i = 0; i < listOfNodes.size(); i++) {

                                MsgToSend msg1 = new MsgToSend();
                                msg1.nodeId = listOfNodes.get(i);
                                if (i == 0) {
                                    //predescc of 1st element is the last element in the network
                                    msg1.predes = listOfNodes.get(listOfNodes.size() - 1);
                                    msg1.successor = listOfNodes.get(i + 1);

                                } else if (i == (listOfNodes.size() - 1)) {
                                    //successor of last element is the first element in the network
                                    msg1.predes = listOfNodes.get(i - 1);
                                    msg1.successor = listOfNodes.get(0);
                                } else {
                                    msg1.predes = listOfNodes.get(i - 1);
                                    msg1.successor = listOfNodes.get(i + 1);
                                }

                                msg1.msgType = RING_ACK;
                                //Notify each one of the ring with its predesscors
                          //      Log.d("Server", "My nodeId is----" + msg1.nodeId + "---My predes is--" + msg1.predes + "---My success is--" + msg1.successor);

                                Log.d("ACTUAL", "My nodeId is----" + totalRingNodesTemp.get(msg1.nodeId) + "---My predes is--" + totalRingNodesTemp.get(msg1.predes) + "---My success is--" + totalRingNodesTemp.get(msg1.successor));

                                Socket replySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        (Integer.parseInt(totalRingNodesTemp.get(msg1.nodeId)) * 2));

                                ObjectOutputStream reply = new ObjectOutputStream(replySocket.getOutputStream());
                                reply.writeObject(msg1);
                                reply.flush();
                                reply.close();
                            }
                        }

                    } else if (msgRecvd.msgType.equalsIgnoreCase(RING_ACK)) {
                    /*The other nodes receiving their position in the ring*/
                        RING_FORMED = true;
                        trck.setNodeId(msgRecvd.nodeId);
                        trck.setPredecessor(msgRecvd.predes);
                        trck.setSuccessor(msgRecvd.successor);
                        Log.d("RING FORMED", "Line 250");
                        Log.d("getPredecessor actual   ", totalRingNodesTemp.get(trck.getPredecessor()));
                        Log.d("getSuccessor actual   ", totalRingNodesTemp.get(trck.getSuccessor()));
                        Log.d("Port is    ", portStr);
                    } else if (msgRecvd.msgType.equalsIgnoreCase(INSERT_DATA) ||
                            msgRecvd.msgType.equalsIgnoreCase(SEARCH_KEY)) {
                    /*When the data is to be stored in the proper node*/
                    /*when data insert request comes directly to 5554..it has all the list of the nodes in the ring*/
                        if (portStr.equalsIgnoreCase("5554")) {
                            String mkey = msgRecvd.msgKey;
                            //String value = msgRecvd.msgValue;
                            String destinationNode = null;
                            //To get the hashed key
                            String key = null;
                            try {
                                key = genHash(mkey);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e("error", "In insert function" + e);
                            }

                            for (int l = 0; l < listOfNodes.size(); l++) {
                                String tempNode = listOfNodes.get(l);
                                int result = tempNode.compareTo(key);

                                //a value less than 0 if the argument is a string lexicographically greater than this string;
                                // and a value greater than 0 if the argument is a string lexicographically less than this string.
                                if (result < 0) {
                                    //key is greater than nodeId
                                    if (l != (listOfNodes.size() - 1)) {
                                        continue;
                                    } else {
                                        //It is the last node, and key is still greater than node Id. hence key goes to the first node
                                        destinationNode = listOfNodes.get(0);
                                        break;
                                    }
                                } else {
                                    //key is less than or equal to nodeId
                                    if (l == 0) {
                                        destinationNode = listOfNodes.get(0);
                                    } else {
                                        destinationNode = listOfNodes.get(l);
                                    }
                                    break;
                                }
                            }

                            String destinationPort = totalRingNodesTemp.get(destinationNode);
                            Log.d("Printing key values%%%","Key---"+mkey+"  Hashed  "+key+"  DestinationPort---"+destinationPort);

                            Socket replySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(destinationPort) * 2));

                            ObjectOutputStream reply = new ObjectOutputStream(replySocket.getOutputStream());
                            // public MsgToSend(String msgType, String senderPort, String predes, String successor, String nodeId, String msgKey, String msgValue) {
                            MsgToSend msg1=null;
                            if(msgRecvd.msgType.equalsIgnoreCase(INSERT_DATA)) {
                                msg1 = new MsgToSend(IN_NODE, portStr, null, null, null, mkey, msgRecvd.msgValue, null, null);
                            }else if(msgRecvd.msgType.equalsIgnoreCase(SEARCH_KEY)){
                                Log.d("key found","key found  "+mkey);
                                msg1 = new MsgToSend(SEARCH_KEY_ANS,msgRecvd.senderPort, null, null, null, mkey, null, null, null);

                            }
                            reply.writeObject(msg1);
                            reply.flush();
                            reply.close();
                        }
                    } else if (msgRecvd.msgType.equalsIgnoreCase(IN_NODE)) {
                        //The proper node has the data. Now only thing is to insert data in the content provider

                        String key = msgRecvd.msgKey;
                        String value = msgRecvd.msgValue;
                        databaseInsert(key, value);
                        Log.d("Message inserted in the database from ring","Key---"+key+"  port---"+portStr);
                    }
                    else if (msgRecvd.msgType.equalsIgnoreCase(SEARCH_KEY_ANS)) {

                        String key = msgRecvd.msgKey;
                        Log.d("Searching for value","Search value in port "+ portStr+" for key "+key);

                        String whereclause = DataBaseConnector.TABLE_COLUMN1 + " = '" + key + "'";
                        Cursor res = db.query(DataBaseConnector.TABLE_NAME, null, whereclause, null, null, null, null);
                        Log.d("here","here");
                        res.moveToFirst();
                        HashMap<String,String> hm =new HashMap<String,String>();
                        hm.put(key, res.getString(res.getColumnIndex(VALUE_FIELD)));

                        //valueNotReturned =  true;

                        Log.d("Message retrieved from the database from ring","Key---"+key+" value  "+res.getString(res.getColumnIndex(VALUE_FIELD))+"  port---"+portStr);

                        MsgToSend m2 = new MsgToSend();
                        m2.msgType = DONE;
                        m2.msgKey = key;
                        m2.message.put(key,res.getString(res.getColumnIndex(VALUE_FIELD)));


                        Socket replyQSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (Integer.parseInt(msgRecvd.senderPort) * 2));

                        ObjectOutputStream replyQ = new ObjectOutputStream(replyQSocket.getOutputStream());
                        replyQ.writeObject(m2);
                        replyQ.flush();
                        replyQ.close();
                    }else if (msgRecvd.msgType.equalsIgnoreCase(DONE)) {

                        Log.d("anything", "anything my port is "+portStr);
                        Log.d("message", "key "+msgRecvd.msgKey+"  value  "+msgRecvd.message.get(msgRecvd.msgKey));
                       // wholeResult.putAll(msgRecvd.message);
                        wholeResult.put(msgRecvd.msgKey, msgRecvd.message.get(msgRecvd.msgKey));
                        valueNotReturned =  true;

                    }else if (msgRecvd.msgType.equalsIgnoreCase(STAR_Q)) {

                        Log.d("STAR QUERY","Line 609 the query from each avd begins");
                        String query = "SELECT  * FROM " + DataBaseConnector.TABLE_NAME;
                        Cursor cursor = db.rawQuery(query, null);

                        int keyIndex = cursor.getColumnIndex(KEY_FIELD);
                        int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
                        if (cursor.moveToFirst()) {
                            do {
                                HashMap<String,String> hm= new HashMap<>();
                                hm.put(cursor.getString(keyIndex), cursor.getString(valueIndex));
                                if(msgRecvd.getMessage()!=null)
                                {
                                    hm.putAll(msgRecvd.getMessage());
                                }
                                msgRecvd.setMessage(hm);
                                Set set = msgRecvd.message.entrySet();
                                // Get an iterator
                                Iterator i = set.iterator();
                                // Display elements
                                while(i.hasNext()) {
                                    Log.d("Star query", "Mid of the ring port " +portStr);
                                    Map.Entry me = (Map.Entry)i.next();
                                    Log.d("key "+me.getKey() , "value " +me.getValue());
                                }
                                Log.d("Put in hashmap", "After query");
                            } while (cursor.moveToNext());
                        }

                        if (!portStr.equalsIgnoreCase(msgRecvd.senderPort)) {

                            //Putting the successor value to which the packet would be mext sent
                            msgRecvd.receiverPort = totalRingNodesTemp.get(trck.getSuccessor());
                            Log.d("Next in line ","Star query port "+ msgRecvd.receiverPort);

                            Socket replyQSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(msgRecvd.receiverPort) * 2));

                            ObjectOutputStream replyQ = new ObjectOutputStream(replyQSocket.getOutputStream());
                            replyQ.writeObject(msgRecvd);
                            replyQ.flush();
                            replyQ.close();
                        } else {
                            Log.d("Star query ends", "Ring is completly traversed");
                            TRAVEL_FULL_RING = true;

                            try {
                                Log.d("star query","Line 649");

                                wholeResult.putAll(msgRecvd.message);
                            }catch(NullPointerException e){
                                Log.d("nullpointer","star query line 653");
                            }
                            msgRecvd.receiverPort = portStr;
                        }
                    }else if (msgRecvd.msgType.equalsIgnoreCase(DELETE_REQ)) {

                        Log.d("DELETE STAR QUERY","Line 781 delete from each avd begins");

                        int value = db.delete(DataBaseConnector.TABLE_NAME, null,null);
                        Log.d("DELETE STAR QUERY","Line 781 delete from each avd begins value" + value+" port "+portStr);

                        if (!portStr.equalsIgnoreCase(msgRecvd.senderPort)) {
                            //Putting the successor value to which the packet would be mext sent
                            msgRecvd.receiverPort = totalRingNodesTemp.get(trck.getSuccessor());
                            Log.d("Next in line ","Star query port "+ msgRecvd.receiverPort);

                            Socket replyQSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(msgRecvd.receiverPort) * 2));

                            ObjectOutputStream replyQ = new ObjectOutputStream(replyQSocket.getOutputStream());
                            replyQ.writeObject(msgRecvd);
                            replyQ.flush();
                            replyQ.close();

                        } else {
                            Log.d("Star DELETE query ends", "Ring is completly traversed");

                            TRAVEL_FULL_RING_DELETE = true;
                            msgRecvd.receiverPort = portStr;
                            }
                    } else if (msgRecvd.msgType.equalsIgnoreCase(SEARCH_KEY_TO_DELETE)) {
                    /*When the data is to be deleted from the proper node*/
                    /*when data insert request comes directly to 5554..it has all the list of the nodes in the ring*/
                        if (portStr.equalsIgnoreCase("5554")) {

                            String mkey = msgRecvd.msgKey;
                            //String value = msgRecvd.msgValue;
                            String destinationNode = null;
                            //To get the hashed key
                            String key = null;
                            try {
                                key = genHash(mkey);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e("error", "In delete function" + e);
                            }

                            for (int l = 0; l < listOfNodes.size(); l++) {
                                String tempNode = listOfNodes.get(l);
                                int result = tempNode.compareTo(key);
                                //a value less than 0 if the argument is a string lexicographically greater than this string;
                                // and a value greater than 0 if the argument is a string lexicographically less than this string.
                                if (result < 0) {
                                    //key is greater than nodeId
                                    if (l != (listOfNodes.size() - 1)) {
                                        continue;
                                    } else {
                                        //It is the last node, and key is still greater than node Id. hence key goes to the first node
                                        destinationNode = listOfNodes.get(0);
                                        break;
                                    }
                                } else {
                                    //key is less than or equal to nodeId
                                    if (l == 0) {
                                        destinationNode = listOfNodes.get(0);
                                    } else {
                                        destinationNode = listOfNodes.get(l);
                                    }
                                    break;
                                }
                            }


                            String destinationPort = totalRingNodesTemp.get(destinationNode);
                            Log.d("Printing key DELETE%%%","Key---"+mkey+"  DestinationPort---"+destinationPort+" Sender Port "+msgRecvd.senderPort);

                            Socket replySocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    (Integer.parseInt(destinationPort) * 2));

                            ObjectOutputStream reply = new ObjectOutputStream(replySocket.getOutputStream());
                            // public MsgToSend(String msgType, String senderPort, String predes, String successor, String nodeId, String msgKey, String msgValue) {
                            MsgToSend msg1= new MsgToSend(SEARCH_KEY_TO_DELETE_ANS, msgRecvd.senderPort, null, null, null, mkey, msgRecvd.msgValue, null, null);

                            Log.d("SEARCH_KEY_TO_DELETE","key found  "+mkey);

                            reply.writeObject(msg1);
                            reply.flush();
                            reply.close();


                        }
                    }
                    else if (msgRecvd.msgType.equalsIgnoreCase(SEARCH_KEY_TO_DELETE_ANS)) {

                        String key = msgRecvd.msgKey;
                        Log.d("DELETING value","Search value in port "+ portStr+" for key "+key);

                        String whereclause = DataBaseConnector.TABLE_COLUMN1 + " = '" + key + "'";
                        int res = db.delete(DataBaseConnector.TABLE_NAME, whereclause, null);

                        //int valueIndex = res.getColumnIndex(VALUE_FIELD);
                        //valueNotReturneddelete=true;
                        Log.d("DELETED_SINGLE_KEY ","DELETED_SINGLE_KEY  " +key+ "from port"+portStr);

                        MsgToSend m2 = new MsgToSend();
                        m2.msgType = DELETED_SINGLE_KEY;
                        m2.msgKey = key;
                        Socket replyQSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (Integer.parseInt(msgRecvd.senderPort) * 2));

                        ObjectOutputStream replyQ = new ObjectOutputStream(replyQSocket.getOutputStream());
                        replyQ.writeObject(m2);
                        replyQ.flush();
                        replyQ.close();
                    }else if(msgRecvd.msgType.equalsIgnoreCase(DELETED_SINGLE_KEY)){

                        Log.d("DELETED_SINGLE_KEY ","Back to the thread from where it started for key  " +msgRecvd.msgKey +" from port"+portStr);
                        valueNotReturneddelete=true;
                    }
                ois.close();
            }
            } catch (IOException e) {
                e.printStackTrace();
            }catch(ClassNotFoundException e){
                Log.d("FATAL",e.getMessage());
            }
            return null;
        }
    }

    private void helperFunction(MsgToSend msgh) throws UnknownHostException,IOException{

        int port = 11108;

        if(msgh.msgType.equalsIgnoreCase(STAR_Q)){
            port=Integer.parseInt(msgh.receiverPort)*2;
            Log.d("STAR QUERY from client","Request to be sent to port"+port);
        }
        Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                port);

        ObjectOutputStream out=new ObjectOutputStream(clientSocket.getOutputStream());
        out.writeObject(msgh);
        out.flush();

        Log.d("Client","Message type is --"+msgh.msgType+"Client request sent from "+portStr);
        out.close();

    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {

                Log.d("Client","Inside client class" +portStr);
                // public MsgToSend(String msgType, String senderPort, String predes, String successor, String nodeId, String msgKey, String msgValue) {
                MsgToSend msg =new MsgToSend(RING_REQ,portStr,null,null,null,null,null,null,null);
                //giving own portid

           //     Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
             //           (Integer.parseInt("5554")*2));

                helperFunction(msg);/*
                String msg_new =msgs[0];
                if(msg_new.equalsIgnoreCase(INSERT_DATA)){
                    //Just forwarding the data to 5554
                     msg =new MsgToSend(INSERT_DATA,portStr,null,null,null,null,null);
                }





                    Socket clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        11108);

                ObjectOutputStream out=new ObjectOutputStream(clientSocket.getOutputStream());
                out.writeObject(msg);
                out.flush();

                Log.d("Client","Request to form ring sent from"+portStr);
                out.close();
*/

            } catch (UnknownHostException e) {
                Log.e("Error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Error", "ClientTask socket IOException" + e.getMessage());
            }

            return null;
        }

    }



    private void populateHashedPorts(){

        try{
            for(int k=0;k<5;k++) {
                totalRingNodesTemp.put(genHash(PORTS[k]),PORTS[k]);
            }
        }catch(NoSuchAlgorithmException e){
            Log.e("Hashing error",e.getMessage());
        }
 }


    private String getHashedPorts(String nodeid){

        try{
            for(int k=0;k<5;k++) {
                totalRingNodes.put(genHash(PORTS[k]),PORTS[k]);
            }

            return totalRingNodes.get(nodeid);
        }catch(NoSuchAlgorithmException e){
            Log.e("Hashing error",e.getMessage());
        }
        return null;
    }
}