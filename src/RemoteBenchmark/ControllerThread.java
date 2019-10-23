/**
 *
 */
package RemoteBenchmark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * @author Karthik Badarinath
 *
 */
public class ControllerThread extends Thread {

	String WorkingIP;
	Boolean Search;
	ArrayList<String> SendPacket;

	public ControllerThread(String dbName, int ReadPercentage, int NoOfDoc, int NoOfOpPerThread, String IPAddress, ArrayList<String> MasterIPAddress, String SearchString, Boolean MapReduce){

		WorkingIP = IPAddress;
		Search = MapReduce;
		SendPacket = new ArrayList<String>();
		SendPacket.add(dbName);
		SendPacket.add(String.valueOf(ReadPercentage));
		SendPacket.add(String.valueOf(NoOfDoc));
		SendPacket.add(String.valueOf(NoOfOpPerThread));
		SendPacket.add(SearchString);
		SendPacket.add(String.valueOf(MapReduce));
		for(int i=0;i<MasterIPAddress.size();i++){
			SendPacket.add(MasterIPAddress.get(i));
		}
	}

	@SuppressWarnings({ "unchecked", "resource" })
	public void run(){

		try {
			InetAddress client = InetAddress.getByName(WorkingIP);
			try {
				Socket soc = new Socket(client.getHostAddress(), 9999);
				ObjectOutputStream packetout = new ObjectOutputStream(soc.getOutputStream());
				packetout.writeObject(SendPacket);
				ObjectInputStream packetin = new ObjectInputStream(soc.getInputStream());
				ArrayList<Double> NodeResult;
				try {
					NodeResult = (ArrayList<Double>) packetin.readObject();
					if(!Search){
						ControllerHandler.finalresult.add(NodeResult);
					}
					else{
						MapReduceController.finalresult.add(NodeResult);
					}
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				packetin.close();
				packetout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
