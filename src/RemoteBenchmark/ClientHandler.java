/**
 *
 */
package RemoteBenchmark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import Benchmark.CouchDBBenchmark;
import Benchmark.CouchDBMapReduce;
import Implementation.CouchDB;

/**
 * @author Karthik Badarinath
 *
 */
public class ClientHandler {

	static ServerSocket ser;
	static ObjectInputStream inputstream;
	static ObjectOutputStream outputstream;

	public ClientHandler(){
		try {
			ser = new ServerSocket(9999);
			incomingPacket();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void incomingPacket(){

		System.out.println("\nWaiting for controller to pass the signal...");

		try {
			Socket soc = ser.accept();
			packetProcess(soc);
			incomingPacket();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	public static void packetProcess(Socket soc){

		String dbName = null;
		String SearchString = null;
		String[] IPAddress = {"0"};

		int ReadPercentage = 0;
		int NoOfDoc = 0;
		int NoOfOperation = 0;

		boolean Search = false;

		System.out.println("Control received!");
		try {
			inputstream = new ObjectInputStream(soc.getInputStream());
			ArrayList<String> recPackets;
			try {
				recPackets = (ArrayList<String>) inputstream.readObject();
				dbName = recPackets.get(0);
				ReadPercentage = Integer.parseInt(recPackets.get(1));
				NoOfDoc = Integer.parseInt(recPackets.get(2));
				NoOfOperation = Integer.parseInt(recPackets.get(3));
				SearchString = recPackets.get(4);
				Search = Boolean.valueOf(recPackets.get(5));
				IPAddress[0] = recPackets.get(6);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		CouchDB.session(IPAddress[0]);
		if(!Search){
			CouchDBBenchmark.benchmark(dbName, NoOfOperation, IPAddress, ReadPercentage, NoOfDoc);
		}
		else{
			CouchDBMapReduce.searchBenchmark(IPAddress[0], dbName, ReadPercentage, SearchString);
		}
		try {
			outputstream = new ObjectOutputStream(soc.getOutputStream());
			if(!Search){
				outputstream.writeObject(CouchDBBenchmark.FinalResult);
			}
			else{
				outputstream.writeObject(CouchDBMapReduce.MapResults);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			inputstream.close();
			outputstream.close();
			soc.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
