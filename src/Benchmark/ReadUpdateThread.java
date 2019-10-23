/**
 *
 */
package Benchmark;

import java.util.Random;

import com.fourspaces.couchdb.Session;

import Implementation.CouchDB;

/**
 * @author Karthik
 *
 */
public class ReadUpdateThread extends Thread{

	//Declarations
		String NodeAddress;
		String DBName;

		int TotReadsPerformed = 0;
		int TotUpdatesPerformed = 0;
		int NoOfReads;
		int NoOfOp;
		int NoOfDocs;

		Session conn;

		//Used in entire page
		Random rndno = new Random();

	public ReadUpdateThread(String dbName, int OperationsPerThread, String IPAddress, int ReadPercentage, int NoOfDoc){
		DBName = dbName;
		NoOfOp = OperationsPerThread;
		NodeAddress = IPAddress;
		NoOfReads = ReadPercentage;
		NoOfDocs = NoOfDoc;
		conn = CouchDB.session(NodeAddress);
	}

	public void run(){

		//Declarations
		int ReadFailed = 0, UpdateFailed = 0;

		//Actual ReadOperations and Update Operations

		for(int i = 0; i<NoOfOp; i++){
			int InsertKey = rndno.nextInt(NoOfDocs)+1;
			String doc =  CouchDB.read(conn, DBName, String.valueOf(InsertKey));
			if(doc == null){
				System.out.println("Document number that failed to read is "+InsertKey+"...");
				ReadFailed++;
			}
			int UpdateKey = rndno.nextInt(100);
			if(UpdateKey > NoOfReads){
				int ret = CouchDB.update(conn, DBName, doc, String.valueOf(InsertKey));
				TotUpdatesPerformed++;

				if(ret!=1){
					System.out.println("Document number that failed to update is "+UpdateKey+"...");
					UpdateFailed++;
					TotUpdatesPerformed = TotUpdatesPerformed - 1;
				}
			}
			TotReadsPerformed++;
		}
		System.out.println("Completed "+TotReadsPerformed+" reads and "+TotUpdatesPerformed+" update operations...");
		System.out.println(ReadFailed+" reads failed and "+UpdateFailed+" update failed!");
	}
}
