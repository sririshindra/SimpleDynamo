package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

	private Uri providerUri;

	static final int SERVER_PORT = 10000;
	static String my_port = null;
	static List<String> listOfNodesInDynamo = null;
	static TreeSet<String> abc = new TreeSet<String>();
	Map<String, String> hashMap = new HashMap<String, String>();
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] addresses = new String[]{"5562", "5556", "5554","5558", "5560"};

	//static final String[] ports = new String[]{"11108","11112","11116", "11120","11124"};

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String message = "delete" + "_" + selection;
		for (String address: addresses){
			sendMessageToNode(message, address);
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String key = values.get("key").toString();
		String value = values.get("value").toString();
		String coordinatorAddress = getMsgAddress(key);
		String successorArray[] = getSucessors(coordinatorAddress).split("_");
		String succ1 = successorArray[0];
		String succ2 = successorArray[1];
		int replicationFactor = 1;
		String queryMessage = "query" + "_" + key + "_" + replicationFactor;

		String response1 = null;
		if (coordinatorAddress.equalsIgnoreCase(my_port)){
			response1 = handleQueryReplicationFactorOne(key);
		} else {
			response1 = sendMessageToNode(queryMessage, coordinatorAddress);
		}

		String response2 = null;
		if (succ1.equalsIgnoreCase(my_port)){
			response2 = handleQueryReplicationFactorOne(key);
		} else {
			response2 = sendMessageToNode(queryMessage, succ1);
		}

		String response3 = null;
		if (succ2.equalsIgnoreCase(my_port)){
			response3 = handleQueryReplicationFactorOne(key);
		} else {
			response3 = sendMessageToNode(queryMessage, succ2);
		}

		String version_value = getLatestVersionAndMsg(key, response1, response2, response3);
		String[] version_valueArray = version_value.split("_");
		String insert_message = "insert" + "_" + values.get("key") + "_" + value
				+ "_" + coordinatorAddress+ "_" + replicationFactor + "_" + version_valueArray[0];

		if (coordinatorAddress.equalsIgnoreCase(my_port)){
			handleInsertReplicationFactorOne(version_valueArray[0], key, value, coordinatorAddress);
		} else {
			sendMessageToNode(insert_message, coordinatorAddress);
		}

		if (succ1.equalsIgnoreCase(my_port)){
			handleInsertReplicationFactorOne(version_valueArray[0], key, value, coordinatorAddress);
		} else {
			sendMessageToNode(insert_message, succ1);
		}

		if (succ2.equalsIgnoreCase(my_port)){
			handleInsertReplicationFactorOne(version_valueArray[0], key, value, coordinatorAddress);
		} else {
			sendMessageToNode(insert_message, succ2);
		}

		return null;
	}


	private String sendMessageToNode(String message, String address){

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(address) * 2);
			socket.setSoTimeout(500);
			PrintWriter pwOutStream = new PrintWriter(socket.getOutputStream(), true);
			pwOutStream.println(message);
			pwOutStream.flush();
			Log.i("TAG", "message to be sent in sendMessageToNode " + message
					+ " and address is " + address);
			BufferedReader brInStream = new BufferedReader(
					new InputStreamReader(socket.getInputStream()));
			String response = brInStream.readLine();
			Log.i("TAG", "response received from message is " + response + "  " + address);
			Log.v("TAG", response);
			pwOutStream.close();
			brInStream.close();
			socket.close();
			return response;

		} catch (SocketTimeoutException e){
			Log.e("TAG", "SocketTimeoutException in sendMessageToNode " + e.getMessage());
			e.printStackTrace();

		} catch (NullPointerException e){
			Log.e("TAG", "NullPointerException in sendMessageToNode " + e.getMessage());
			e.printStackTrace();

		} catch (IOException e){
			Log.e("TAG", "IOException in sendMessageToNode " + e.getMessage());
			e.printStackTrace();

		} catch (Exception e){
			Log.e("TAG", "Exception in sendMessageToNode " + e.getMessage());
			e.printStackTrace();

		}

		return null;
	}

	@Override
	public boolean onCreate() {
		Log.i(TAG, " onCreate is called ");
		my_port=getMyPortValue();
		populateListOfNodesInDynamo();
		if (checkIfIAmRecoveredNode()){
			Log.i(TAG, " I am a recovered node");
			runRecoveryProcedures();
		} else {
			Log.i(TAG, " I am a new node");
			markMeAsAnOldNode();
		}
		startServerTask();
		return true;
	}

	private void startServerTask(){
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
	}

	private void populateListOfNodesInDynamo() {
		listOfNodesInDynamo = new ArrayList<String>();
		listOfNodesInDynamo.add("5562_177ccecaec32c54b82d5aaafc18a2dadb753e3b1");
		listOfNodesInDynamo.add("5556_208f7f72b198dadd244e61801abe1ec3a4857bc9");
		listOfNodesInDynamo.add("5554_33d6357cfaaf0f72991b0ecd8c56da066613c089");
		listOfNodesInDynamo.add("5558_abf0fd8db03e5ecb199a9b82929e9db79b909643");
		listOfNodesInDynamo.add("5560_c25ddd596aa7c81fa12378fa725f706d54325d12");

	}

	private String getSucessors(String address){

		String succ1 = null;
		String succ2 = null;
		for (int counter = 0; counter < 5; counter++){
			String node_hash_array[] = listOfNodesInDynamo.get(counter).split("_");
			String current_node = node_hash_array[0];
			String current_hash = node_hash_array[1];
			if(address.equalsIgnoreCase(current_node)){
				if (counter == 3){
					succ1 = listOfNodesInDynamo.get(4).split("_")[0];
					succ2 = listOfNodesInDynamo.get(0).split("_")[0];
				}  else if (counter == 4){
					succ1 = listOfNodesInDynamo.get(0).split("_")[0];
					succ2 = listOfNodesInDynamo.get(1).split("_")[0];
				} else {
					succ1 = listOfNodesInDynamo.get(counter + 1).split("_")[0];
					succ2 = listOfNodesInDynamo.get(counter + 2).split("_")[0];
				}
			}
		}
		return succ1 + "_" + succ2;
	}

	private String getPredecessors(String address){
		String pred1 = null;
		String pred2 = null;
		for (int counter = 0; counter < 5; counter++){
			String node_hash_array[] = listOfNodesInDynamo.get(counter).split("_");
			String current_node = node_hash_array[0];
			String current_hash = node_hash_array[1];
			if(address.equalsIgnoreCase(current_node)){
				if (counter == 0){
					pred1 = listOfNodesInDynamo.get(4).split("_")[0];
					pred2 = listOfNodesInDynamo.get(3).split("_")[0];
				}  else if (counter == 1){
					pred1 = listOfNodesInDynamo.get(0).split("_")[0];
					pred2 = listOfNodesInDynamo.get(4).split("_")[0];
				} else {
					pred1 = listOfNodesInDynamo.get(counter - 1).split("_")[0];
					pred2 = listOfNodesInDynamo.get(counter - 2).split("_")[0];
				}
			}
		}

		return pred1 + "_" + pred2;
	}

	private String getMsgAddress(String message) {

		String address = null;

		for (int counter = 0; counter < 5; counter++){

			String node_hash_array[] = listOfNodesInDynamo.get(counter).split("_");
			String current_node = node_hash_array[0];
			String current_hash = node_hash_array[1];
			String prevNodeArray[] = null;
			String prevNode = null;
			String prevNodehash = null;

			if (counter == 0){
				prevNodeArray = listOfNodesInDynamo.get(4).split("_");
			} else if (counter == 4){
				prevNodeArray = listOfNodesInDynamo.get(3).split("_");
			} else {
				prevNodeArray = listOfNodesInDynamo.get(counter - 1).split("_");
			}
			prevNode = prevNodeArray[0];
			prevNodehash = prevNodeArray[1];

			if (isMessageAddressedToMe(message, prevNodehash, current_hash)){
				address = current_node;
				break;
			}

		}
		return address;

	}

	private boolean isMessageAddressedToMe(String message, String predecessorId, String nodeId) {
		//checks whether the message addressed to me i.e, finding the position in the ring
		String messageToBeCompared = null;
		try {
			messageToBeCompared = genHash(message);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		if (predecessorId.equalsIgnoreCase(nodeId)) {
			return true;
		}
		Log.i(TAG, "message in isMessageAddressedToMe is " + message);
		Log.i(TAG, "messageToBeCompared is " + messageToBeCompared);
		Log.i(TAG, "predecessorId is " + predecessorId);
		Log.i(TAG, "nodeId is " + nodeId);


		if (nodeId.compareTo(predecessorId) < 0) {
			Log.i(TAG, "entered first if ");
			if (messageToBeCompared.compareTo(predecessorId) > 0 ||
					messageToBeCompared.compareTo(nodeId) < 0) {
				Log.i(TAG, "entered second if ");
				return true;
			} else {
				Log.i(TAG, "entered first else ");
				return false;
			}
		} else {
			Log.i(TAG, "entered second else ");
			if (messageToBeCompared.compareTo(predecessorId) > 0) {
				Log.i(TAG, "entered third if ");
				if (messageToBeCompared.compareTo(nodeId) < 0) {
					Log.i(TAG, "entered fourth if ");
					return true;
				} else {
					Log.i(TAG, "entered third else ");
					return false;
				}
			} else {
				Log.i(TAG, "entered fourth else ");
				return false;
			}
		}


	}

	private void runRecoveryProcedures() {
		//TODO implement method runRecoveryProcedures
		try {
			new TakeMyKeyValuePairsFromSuccessors().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
			new TakeMyKeyValuePairsFromPredecessors().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

	}

	private class TakeMyKeyValuePairsFromSuccessors extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... strings) {
			String[] successorArray = getSucessors(my_port).split("_");
			String succ1 = successorArray[0];
			String succ2 = successorArray[1];
			String response1 = sendMessageToNode("giveMeMyData_" + my_port, succ1);
			String response2 = sendMessageToNode("giveMeMyData_" + my_port, succ2);
			try {
				JSONObject tempKeyValuePairs1 = new JSONObject(response1);
				JSONObject tempKeyValuePairs2 = new JSONObject(response2);
				Set<String> keys = new HashSet<String>();
				Iterator tempKeyValuePairsIter = tempKeyValuePairs1.keys();
				while (tempKeyValuePairsIter.hasNext()){
					keys.add(tempKeyValuePairsIter.next().toString());
				}
				tempKeyValuePairsIter = tempKeyValuePairs2.keys();
				while (tempKeyValuePairsIter.hasNext()){
					keys.add(tempKeyValuePairsIter.next().toString());
				}
				Iterator<String> keysIterator = keys.iterator();
				while (keysIterator.hasNext()) {
					String key = keysIterator.next();
					String value1 = null;
					String value2 = null;
					try {
						value1 = tempKeyValuePairs1.getString(key);
					} catch (Exception e) {
						e.printStackTrace();
					}

					try {
						value2 = tempKeyValuePairs2.getString(key);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (value1 == null && value2 == null){
						Log.e(TAG, "both value1 and value2 became null in TakeMyKeyValuePairsFromSuccessors");
					} else if (value1 == null){
						hashMap.put(key, value2);
					} else if (value2 == null){
						hashMap.put(key, value1);
					} else {
						int version1 = Integer.parseInt(value1.split("_")[0]);
						int version2 = Integer.parseInt(value2.split("_")[0]);
						if (version1 > version2){
							hashMap.put(key, value1);
						} else {
							hashMap.put(key, value2);
						}
					}

				}


			} catch (JSONException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class TakeMyKeyValuePairsFromPredecessors extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... strings) {
			String[] predecessorArray = getPredecessors(my_port).split("_");
			String pred1 = predecessorArray[0];
			String pred2 = predecessorArray[1];
			String response1 = sendMessageToNode("giveMeYourData", pred1);
			String response2 = sendMessageToNode("giveMeYourData", pred2);

			try {
				JSONObject tempKeyValuePairs = new JSONObject(response1);
				Iterator tempKeyValuePairsIter = tempKeyValuePairs.keys();
				while (tempKeyValuePairsIter.hasNext()){
					String key = tempKeyValuePairsIter.next().toString();
					hashMap.put(key, tempKeyValuePairs.getString(key));
				}

				tempKeyValuePairs = new JSONObject(response2);
				tempKeyValuePairsIter = tempKeyValuePairs.keys();
				while (tempKeyValuePairsIter.hasNext()){
					String key = tempKeyValuePairsIter.next().toString();
					hashMap.put(key, tempKeyValuePairs.getString(key));
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	private boolean checkIfIAmRecoveredNode() {

		String fileName = "nodeStatus";
		try {
			FileInputStream inputSTream = getContext().openFileInput(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.i(TAG, " nodeStatus file is not found. There fore I am a new node");
			return false;
		}
		Log.i(TAG, " nodeStatus file is found. Therefore I am an old node");
		return true;
	}

	private void markMeAsAnOldNode() {

		String fileName = "nodeStatus";
		String fileContents = "OLD";
		FileOutputStream outputStream;

		try {
			outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
			outputStream.write(fileContents.getBytes());
			outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private String getMyPortValue() {

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(
				Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return portStr;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		String key =selection;
		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
		int replicationFactor = 1;
		String queryMessage = "query" + "_" + key + "_" + replicationFactor;

		if (selection.equalsIgnoreCase("@")){
			String queryResponse = sendMessageToNode(queryMessage, my_port);
			insertToMatrixCursor(matrixCursor, queryResponse);
		} else if (selection.equalsIgnoreCase("*")){
			for (String address: addresses){
				String response = sendMessageToNode(queryMessage, address);
				if (response != null){
					insertToMatrixCursor(matrixCursor, response);
				}
			}
		} else {
			String coordinatorAddress = getMsgAddress(selection);
			String successorArray[] = getSucessors(coordinatorAddress).split("_");
			String succ1 = successorArray[0];
			String succ2 = successorArray[1];

			String response1 = null;
			if (coordinatorAddress.equalsIgnoreCase(my_port)){
				response1 = handleQueryReplicationFactorOne(key);
			} else {
				response1 = sendMessageToNode(queryMessage, coordinatorAddress);
			}

			String response2 = null;
			if (succ1.equalsIgnoreCase(my_port)){
				response2 = handleQueryReplicationFactorOne(key);
			} else {
				response2 = sendMessageToNode(queryMessage, succ1);
			}

			String response3 = null;
			if (succ2.equalsIgnoreCase(my_port)){
				response3 = handleQueryReplicationFactorOne(key);
			} else {
				response3 = sendMessageToNode(queryMessage, succ2);
			}

			String version_value = getLatestVersionAndMsg(key, response1, response2, response3);
			String[] version_valueArray = version_value.split("_");

			JSONObject tempKeyValuePairs = new JSONObject();

			try {
				if (version_valueArray.length == 2){
//						tempKeyValuePairs.put(selection, version_valueArray[1]);
					tempKeyValuePairs.put(selection, version_value);
					String staleDataUpdate = "stale" + "_" + selection + "_" + version_value + "_" + coordinatorAddress;
					sendMessageToNode(staleDataUpdate, coordinatorAddress);
					sendMessageToNode(staleDataUpdate, succ1);
					sendMessageToNode(staleDataUpdate, succ2);

				}
			} catch (JSONException e) {
				e.printStackTrace();
			}

			insertToMatrixCursor(matrixCursor, tempKeyValuePairs.toString());

//			String queryResponse = sendMessageToNode(queryMessage+ "_" + replicationFactor ,
//					coordinatorAddress);
//			if (queryResponse == null){
//				queryResponse = sendMessageToNode(queryMessage+ "_" + (replicationFactor -1), succ1);
//				insertToMatrixCursor(matrixCursor, queryResponse);
//			} else {
//				insertToMatrixCursor(matrixCursor, queryResponse);
//			}
		}


		return matrixCursor;
	}

	private void insertToMatrixCursor(MatrixCursor matrixCursor, String response) {
		Log.i(TAG, "insertToMatrixCursor is called and the response is " + response);
		try {
			JSONObject keyValuePairs = new JSONObject(response);
			Iterator keyValuePairsIter = keyValuePairs.keys();
			while (keyValuePairsIter.hasNext()){
				String key = keyValuePairsIter.next().toString();
				String value = keyValuePairs.getString(key).split("_")[1];
				matrixCursor.addRow(new String[]{key, value});
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {

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
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			try {
				String message;
				while (true){
					Socket client_socket = serverSocket.accept();
					Log.i(TAG, " accepted client_socket is " + client_socket);
					BufferedReader brInStream = new BufferedReader(
							new InputStreamReader(client_socket.getInputStream()));
					if ((message = brInStream.readLine()) != null) {
						Log.i(TAG, " entered server task method with message " + message);
						if(message.startsWith("query")){
							handleServerTaskQuery(client_socket, message);
						} else if(message.startsWith("insert")){
							handleServerTaskInsert(client_socket, message);
						} else if(message.startsWith("delete")){
							handleServerTaskDelete(client_socket, message);
						} else if(message.startsWith("giveMeMyData")){
							handleGiveMeMyData(client_socket, message);
						} else if(message.startsWith("giveMeYourData")){
							handleGiveMeYourData(client_socket, message);
						} else if(message.startsWith("stale")){
							handleStaleDataUpdate(client_socket, message);
						}
					}
					client_socket.close();
				}

			} catch (IOException e) {
				Log.e(TAG, "Server socket IOException ");
				e.printStackTrace();
			}

			return null;
		}

		private void handleStaleDataUpdate(Socket client_socket, String message) {

			String[] staleDataArray = message.split("_");
			String key = staleDataArray[1];
			int version = Integer.parseInt(staleDataArray[2]);
			String value = staleDataArray[3];
			String coordinatorAddress = staleDataArray[4];
			if (hashMap.get(key) == null){
				hashMap.put(key, version + "_" + value + "_" + coordinatorAddress);
			} else {
				int existingVersion = Integer.parseInt(hashMap.get(key).split("_")[0]);
				if (version > existingVersion){
					hashMap.put(key, version + "_" + value + "_" + coordinatorAddress);
				}
			}
			sendAckToClient(client_socket, "stale data updated");
		}

		private void handleGiveMeYourData(Socket client_socket, String message) {
			Iterator<String> hashMapIter = hashMap.keySet().iterator();
			JSONObject keyValuePairs = new JSONObject();
			while (hashMapIter.hasNext()){
				String key = hashMapIter.next();
				String value = hashMap.get(key);
				String ownerPort = value.split("_")[2];
				if (ownerPort.equalsIgnoreCase(my_port)){
					try {
						keyValuePairs.put(key, value);
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			sendAckToClient(client_socket, keyValuePairs.toString());
		}

		private void handleGiveMeMyData(Socket client_socket, String message) {
			String requesters_port = message.split("_")[1];
			Iterator<String> hashMapIter = hashMap.keySet().iterator();
			JSONObject keyValuePairs = new JSONObject();
			while (hashMapIter.hasNext()){
				String key = hashMapIter.next();
				String value = hashMap.get(key);
				String ownerPort = value.split("_")[2];
				if (ownerPort.equalsIgnoreCase(requesters_port)){
					try {
						keyValuePairs.put(key, value);
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			sendAckToClient(client_socket, keyValuePairs.toString());
		}

		private void handleServerTaskDelete(Socket client_socket, String message) {



			if (message.contains("@") || message.contains("*")){
				hashMap.clear();
			} else {
				hashMap.remove(message.split("_")[1]);
			}
			sendAckToClient(client_socket, message + " deleted");
		}

		private void handleServerTaskInsert(Socket client_socket, String message) {

			Log.i(TAG, "entered handleServerTaskInsert");

			String[] messageArray = message.split("_");
			String[] successorArray = getSucessors(my_port).split("_");
			String succ1 = successorArray[0];
			String succ2 = successorArray[1];
			String key = messageArray[1];
			String value = messageArray[2];
			String coordinatorAddress = messageArray[3];
			int replicationFactor = Integer.parseInt(messageArray[4]);
			String queryMessage = "query" + "_" + key + "_" + 1;
			String version_value = null;
			if (replicationFactor == 3){
//				String response1 = sendMessageToNode(queryMessage, my_port);
				String response1 = handleQueryReplicationFactorOne(key);
				String response2 = sendMessageToNode(queryMessage, succ1);
				String response3 = sendMessageToNode(queryMessage, succ2);
				version_value = getLatestVersionAndMsg(key, response1, response2, response3);
				String[] version_valueArray = version_value.split("_");
				String insert_message = "insert" + "_" + key + "_" + value
						+ "_" + coordinatorAddress + "_" + 1 + "_" + version_valueArray[0];
				handleInsertReplicationFactorOne(version_valueArray[0], key, value, coordinatorAddress);
				sendMessageToNode(insert_message, succ1);
				sendMessageToNode(insert_message, succ2);

			} else if (replicationFactor == 2){
				String response1 = sendMessageToNode(queryMessage, my_port);
				String response2 = sendMessageToNode(queryMessage, succ1);
				version_value = getLatestVersionAndMsg(key, response1, response2);
				String[] version_valueArray = version_value.split("_");
				String insert_message = "insert" + "_" + key + "_" + value
						+ "_" + coordinatorAddress + "_" + 1 + "_" + version_valueArray[0];
				handleInsertReplicationFactorOne(version_valueArray[0], key, value, coordinatorAddress);
				sendMessageToNode(insert_message, succ1);
			} else if (replicationFactor == 1){
				handleInsertReplicationFactorOne(messageArray[5], key, value, coordinatorAddress);
			}

			sendAckToClient(client_socket, "inserted");
		}

		private void handleServerTaskQuery(Socket client_socket, String message) {

			String[] messageArray = message.split("_");
			String selection = messageArray[1];
			JSONObject tempKeyValuePairs = new JSONObject();

			if(selection.equalsIgnoreCase("@") || selection.equalsIgnoreCase("*")){

				Iterator<String> hashMapIter = hashMap.keySet().iterator();
				while (hashMapIter.hasNext()){
					String key = hashMapIter.next();
					try {
						tempKeyValuePairs.put(key, hashMap.get(key));
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}

			} else {

				String[] successorArray = getSucessors(my_port).split("_");
				String succ1 = successorArray[0];
				String succ2 = successorArray[1];
				int replicationFactor = Integer.parseInt(messageArray[2]);
				String version_value = null;
				String queryMessage = "query" + "_" + selection + "_" + 1;
				if (replicationFactor == 3){
					String response1 = handleQueryReplicationFactorOne(selection);
					String response2 = sendMessageToNode(queryMessage, succ1);
					String response3 = sendMessageToNode(queryMessage, succ2);
					version_value = getLatestVersionAndMsg(selection, response1, response2, response3);

				} else if (replicationFactor == 2){
					String response1 = handleQueryReplicationFactorOne(selection);
					String response2 = sendMessageToNode(queryMessage, succ1);
					version_value = getLatestVersionAndMsg(selection, response1, response2);

				} else if (replicationFactor == 1){
					String response = handleQueryReplicationFactorOne(selection);
					version_value = getLatestVersionAndMsg(selection, response);
				}

				String[] version_valueArray = version_value.split("_");

				try {
					if (version_valueArray.length == 2){
//						tempKeyValuePairs.put(selection, version_valueArray[1]);
						tempKeyValuePairs.put(selection, version_value);
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}

			}

			sendAckToClient(client_socket, tempKeyValuePairs.toString());

		}

	}

	private void handleInsertReplicationFactorOne(String string, String key, String value,
												  String coordinatorAddress) {
		int finalVersionNumber = Integer.parseInt(string) + 1;
		hashMap.put(key, finalVersionNumber + "_" + value + "_" + coordinatorAddress);
	}

	private String getLatestVersionAndMsg(String key, String ... responses){
		int version = 0;
		String value = null;
		for (String response : responses){
			if(response != null){
				try {
					JSONObject keyValuePairs = new JSONObject(response);
					Object tempValue = keyValuePairs.get(key);
					if(tempValue != null && !tempValue.toString().equalsIgnoreCase("null")){
						String[] tempValueArray = tempValue.toString().split("_");
//						Log.i(TAG, "tempValue in getLatestVersionAndMsg is " + tempValue );
						int currentVersionNumber = Integer.parseInt(tempValueArray[0]);
						if (currentVersionNumber > version){
							version = currentVersionNumber;
							value = tempValueArray[1];
						}
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
		return version + "_" + value;
	}

	private String handleQueryReplicationFactorOne(String selection) {
		JSONObject tempKeyValuePairs = new JSONObject();
		String value = hashMap.get(selection);
		try {
			if (value != null){
				tempKeyValuePairs.put(selection, value);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return tempKeyValuePairs.toString();
	}

	private void sendAckToClient(Socket client_socket, String ack) {
		PrintWriter pwOutStream;
		try {
			pwOutStream = new PrintWriter(client_socket.getOutputStream(), true);
			pwOutStream.println(ack);
			pwOutStream.flush();
			pwOutStream.close();
		} catch (IOException e) {
			Log.e(TAG, "failed to send ack to client for ack " + ack + " for client "
					+ client_socket);
			e.printStackTrace();
		}

	}
}
