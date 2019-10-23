/**
 *
 */
package RemoteBenchmark;

import java.util.ArrayList;

import Utilities.argsTools;
import Utilities.statTools;

/**
 * @author Karthik
 *
 */
public class MapReduceController {

	public static ArrayList<ArrayList<Double>> finalresult;

	public static void controllerSearch(String dbName, String SearchString, int NoOfRuns, String[] MasterIPAddress, String[] IPAddress){

		ArrayList<ArrayList<String>> DivideOperations;

		System.out.println("\nStarting test, this may take several minutes depending on the specified operation requests, please wait...");
		DivideOperations = argsTools.divideNodeList(IPAddress, MasterIPAddress.length);
		sendPacket(dbName, SearchString, NoOfRuns, DivideOperations, IPAddress, MasterIPAddress);
		statTools statistics = new statTools(finalresult);
		double avg = statistics.getAverage();
		double sd = statistics.getStandardDeviation();
		System.out.println("Average time taken to complete "+NoOfRuns+" runs of search are as follows...");
		System.out.println("Time in Seconds: "+avg+"\nStandard Deviation: "+sd);
		System.out.print("\n");
	}

	public static void sendPacket(String dbName, String StringSearch, int NoOfRuns, ArrayList<ArrayList<String>> DivideOperations, String[] IPAddress, String[] MasterIPAddress){
		ControllerHandler.sendPacket(dbName, NoOfRuns, 1, 1, DivideOperations, IPAddress, MasterIPAddress, StringSearch, true);
	}
}
