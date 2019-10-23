import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import Utilities.InsertDB;
import Benchmark.CouchDBBenchmark;
import Benchmark.CouchDBMapReduce;
import Implementation.CouchDB;
import RemoteBenchmark.ClientHandler;
import RemoteBenchmark.ControllerHandler;
import RemoteBenchmark.MapReduceController;

/**
 * Main class that performs all the operations
 * This class only takes inputs and sends the required information to the relevant operational class in Implementation package
 */

/**
 * @author Karthik Badarinath
 *
 */

public class StartOperations {

	/**
	 * @param args
	 */

	static boolean start = true;

	public static void main(String[] args) throws IOException{

		// TODO Auto-generated method stub

		prints();
	}

	public static void prints(){
		if(start){
			System.out.println("Hello, Welcome to Benchmarking NoSQL program...\n");
			System.out.println("Please choose one of the operations below to start with...");
			start = false;
		}
		else{
			System.out.println("Please select an operation to continue...");
		}
		System.out.println("1. Create database in CouchDB");
		System.out.println("2. Insert data into CouchDB");
		System.out.println("3. Start READ & UPDATE benchmarking");
		System.out.println("4. Start MAPREDUCE benchmarking");
		System.out.println("5. Start Client");
		System.out.println("6. Start READ & UPDATE Controller");
		System.out.println("7. Start MAPREDUCE Controller");
		System.out.println("8. Exit");
		choice();
	}

	public static void choice(){
		int Choice = 0;
		System.out.println("\nEnter your choice: ");
		try{
			BufferedReader choice = new BufferedReader(new InputStreamReader(System.in));
			Choice = Integer.parseInt(choice.readLine());

			if(Choice <= 0  || Choice >= 9){
				System.out.print("\nYou have entered an unlisted choice, please enter an operational choice...\n");
				choice();
			}

			switchChoice(Choice);
		}
		catch(Exception e){
			e.printStackTrace();
			choice();
		}
	}

	public static void switchChoice(int Choice){

		// Declarations

		String ServerName;
		String DatabaseName;
		String ArticlePath;
		String[] IPAddress;
		String[] MasterIPAddress;
		String SearchString;

		int NoOfInserts;
		int UniqueID;
		int NoOfRuns;
		int StartArtNo;
		int NoOfOperations;
		int ReadPercentage;
		int NoOfReadDocs;


		try{
			switch(Choice){
			case 1:
				ServerName = serverName(Choice);
				CouchDB.session(ServerName);
				DatabaseName = createdbName();
				CouchDB.createDB(DatabaseName);
				prints();
			break;
			case 2:
				ServerName = serverName(Choice);
				CouchDB.session(ServerName);
				DatabaseName = createdbName();
				ArticlePath = path();
				NoOfInserts = docInsert();
				NoOfRuns = insertRuns();
				UniqueID = uniqueNumber();
				StartArtNo = articleNo();
				InsertDB.insert(NoOfRuns, NoOfInserts, UniqueID, StartArtNo, ArticlePath, DatabaseName);
				prints();
			break;
			case 3:
				DatabaseName = createdbName();
				NoOfOperations = noOfOperations();
				ReadPercentage = readPercentage();
				NoOfReadDocs = noOfReadDocs();
				IPAddress = ipAddress();
				CouchDBBenchmark.benchmark(DatabaseName, NoOfOperations, IPAddress, ReadPercentage, NoOfReadDocs);
				prints();
			break;
			case 4:
				ServerName = serverName(Choice);
				CouchDB.session(ServerName);
				DatabaseName = createdbName();
				SearchString = searchString();
				NoOfRuns = insertRuns();
				CouchDBMapReduce.searchBenchmark(ServerName, DatabaseName, NoOfRuns, SearchString);
				prints();
			break;
			case 5:
				ClientHandler ch = new ClientHandler();
				ch.incomingPacket();
				prints();
			break;
			case 6:
				DatabaseName = createdbName();
				NoOfOperations = noOfOperations();
				ReadPercentage = readPercentage();
				NoOfReadDocs = noOfReadDocs();
				MasterIPAddress = masterNode();
				IPAddress = ipAddress();
				ControllerHandler.processData(DatabaseName, NoOfOperations, ReadPercentage, NoOfReadDocs, MasterIPAddress, IPAddress);
				prints();
			break;
			case 7:
				DatabaseName = createdbName();
				SearchString = searchString();
				NoOfRuns = insertRuns();
				MasterIPAddress = masterNode();
				IPAddress = ipAddress();
				MapReduceController.controllerSearch(DatabaseName, SearchString, NoOfRuns, MasterIPAddress, IPAddress);
				prints();
			break;
			case 8:
				System.out.println("\nApplication exited!");
				System.exit(0);
			break;
		}
		}catch(ArrayIndexOutOfBoundsException ex){
			System.out.print("\n");
			prints();
		}
	}

	public static String serverName(int Choice){
		String serverAddress = null;
		if(Choice == 4){
			System.out.println("Enter the name of the master node: ");
		}
		else{
			System.out.println("Enter the name of the server: ");
		}
		BufferedReader serveraddress = new BufferedReader(new InputStreamReader(System.in));
		try {
			serverAddress = serveraddress.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return serverAddress;
	}

	public static String createdbName(){
		String dbName = null;
		System.out.println("Enter the name of the database: ");
		BufferedReader dbname = new BufferedReader(new InputStreamReader(System.in));
		try {
			dbName = dbname.readLine().toLowerCase().trim();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dbName;
	}

	public static String path(){
		String dPath = null;
		System.out.println("Enter the path of the article directory: ");
		BufferedReader dpath = new BufferedReader(new InputStreamReader(System.in));
		try {
			dPath = dpath.readLine().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dPath;
	}

	public static int docInsert(){
		int NoOfInserts = 0;
		System.out.println("Enter the number of documents to be inserted: ");
		BufferedReader noofinserts = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfInserts = Integer.parseInt(noofinserts.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return NoOfInserts;
	}

	public static int uniqueNumber(){
		int StartIDinDB = 0;
		System.out.println("Enter the unique ID number that the inserts should start from: ");
		BufferedReader startidindb = new BufferedReader(new InputStreamReader(System.in));
		try {
			StartIDinDB = Integer.parseInt(startidindb.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return StartIDinDB;
	}

	public static int insertRuns(){
		int NoOfInsertRuns = 0;
		System.out.println("Enter the number of runs: ");
		BufferedReader noofinsertruns = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfInsertRuns = Integer.parseInt(noofinsertruns.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return NoOfInsertRuns;
	}

	public static int articleNo(){
		int ArtStartNo = 0;
		System.out.println("Start inserting the data from article number: ");
		BufferedReader artstartno = new BufferedReader(new InputStreamReader(System.in));
		try {
			ArtStartNo = Integer.parseInt(artstartno.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ArtStartNo;
	}

	public static int noOfOperations(){
		int NoOfOperations = 0;
		System.out.println("Enter the number of operations that has to be performed: ");
		BufferedReader noofoperations = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfOperations = Integer.parseInt(noofoperations.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return NoOfOperations;
	}

	public static int readPercentage(){
		int ReadPercentage = 0;
		System.out.println("Enter the read percentage: ");
		BufferedReader readpercentage = new BufferedReader(new InputStreamReader(System.in));
		try {
			ReadPercentage = Integer.parseInt(readpercentage.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ReadPercentage;
	}

	public static int noOfReadDocs(){
		int NoOfReadDocuments = 0;
		System.out.println("Enter the number of the documents to be read: ");
		BufferedReader noofreaddocs = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfReadDocuments = Integer.parseInt(noofreaddocs.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return NoOfReadDocuments;
	}

	public static String[] ipAddress(){
		String IPAddress[] = {"0"};
		int NoOfNode = 0;
		System.out.println("Enter the number of nodes that you are willing to connect: ");
		BufferedReader noofnode = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfNode = Integer.parseInt(noofnode.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Enter the address of the nodes one after the other... ");
		for(int i=0;i<NoOfNode;i++){
			System.out.println("Enter the address of node number " +(i+1)+": ");
			BufferedReader ipaddress = new BufferedReader(new InputStreamReader(System.in));
			try {
				IPAddress[i] = ipaddress.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return IPAddress;
	}

	public static String searchString(){
		String StringSearch = null;
		System.out.println("Enter a string to search: ");
		BufferedReader searchstring = new BufferedReader(new InputStreamReader(System.in));
		try {
			StringSearch = searchstring.readLine().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return StringSearch;
	}

	public static String[] masterNode(){
		String MasterIPAddress[] = {"0"};
		int NoOfNode = 0;
		System.out.println("Enter the number of master nodes that are present in this task: ");
		BufferedReader noofnode = new BufferedReader(new InputStreamReader(System.in));
		try {
			NoOfNode = Integer.parseInt(noofnode.readLine());
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Enter the address of master nodes one after the other... ");
		for(int i=0;i<NoOfNode;i++){
			System.out.println("Enter the address of node number " +(i+1)+": ");
			BufferedReader masteripaddress = new BufferedReader(new InputStreamReader(System.in));
			try {
				MasterIPAddress[i] = masteripaddress.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return MasterIPAddress;
	}
}
