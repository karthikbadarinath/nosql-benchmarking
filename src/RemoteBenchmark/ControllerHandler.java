/**
 *
 */
package RemoteBenchmark;

import java.util.ArrayList;

import Utilities.argsTools;
import Utilities.statTools;

/**
 * @author Karthik Badarinath
 *
 */
public class ControllerHandler {

	public static ArrayList<ArrayList<Double>> finalresult;

	public static void processData(String DatabaseName, int NoOfOperations, int ReadPercentage, int NoOfReadDocs, String[] MasterIPAddress, String[] IPAddress){

		int NoOfOpPerThread;
		int Runs;
		int Allcounts = 0;
		int incoming = 0;

		double deltaTimeMax;

		boolean Mapreduce = false;

		ArrayList<ArrayList<String>> DivideOperations;
		ArrayList<Double> result;

		System.out.println("\nStarting test, this may take several minutes depending on the specified operation requests, please wait...");
		NoOfOpPerThread = NoOfOperations/IPAddress.length;
		DivideOperations = argsTools.divideNodeList(MasterIPAddress, IPAddress.length);
		deltaTimeMax = 200000.0;
		Runs = 5;
		result = new ArrayList<Double>();
		do{
			finalresult = new ArrayList<ArrayList<Double>>();
			sendPacket(DatabaseName, ReadPercentage, NoOfReadDocs, NoOfOpPerThread, DivideOperations, IPAddress, MasterIPAddress, "Nothing", Mapreduce);
			statTools statistics = new statTools(finalresult);
			double avg = statistics.getAverage();
			result.add(avg);
			double time=0;
			int size = result.size();
			if(size > 1){
				time = Math.abs(result.get(size-2) - result.get(size-1));
			}
			System.out.println("Run number "+(Allcounts+1)+" has an average of "+avg+" and standard deviation of "+statistics.getStandardDeviation());
			if(time<=deltaTimeMax){
				incoming++;
			}
			else{
				incoming = 0;
				System.out.println("Stabilization time out, No response from client!");
			}
			Allcounts++;
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while((incoming <= 5) && (Allcounts < Runs));

		System.out.println("\nResults after completing "+Allcounts+" runs to for stabilization...");
		System.out.println("Obtained average time is..\n"+result);
		System.out.print("\n");
	}

	public static void sendPacket(String dbName, int ReadPercentage, int NoOfDoc, int NoOfOpPerThread, ArrayList<ArrayList<String>> DivideOperations, String[] IPAddress, String[] MasterIPAddress, String SearchString, Boolean MapReduce){

		ControllerThread thread[] = new ControllerThread[MasterIPAddress.length];
		int i=0;
		int clientsize = IPAddress.length;
		String clientip;
		while(i<clientsize){
			clientip = IPAddress[i];
			thread[i] = new ControllerThread(dbName, ReadPercentage, NoOfDoc, NoOfOpPerThread, clientip, DivideOperations.get(i), SearchString, MapReduce);
			thread[i].start();
			i++;
		}
		for(int j=0;j<clientsize;j++){
			try {
				thread[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
