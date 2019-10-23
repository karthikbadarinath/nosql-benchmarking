/**
 *
 */
package Benchmark;

import java.util.ArrayList;

import Implementation.CouchDB;

/**
 * @author Karthik Badarinath
 *
 */
public class CouchDBMapReduce {

	public static ArrayList<Double> MapResults;

	public static void searchBenchmark(String ServerName, String dbName, int NoOfRuns, String searchString){

		MapResults = new ArrayList<Double>();
		int retValue = CouchDB.addView(dbName, searchString);
		if(retValue == 0)
		{
			long starttime = System.nanoTime();
			for(int i=0;i<NoOfRuns;i++){
				long time1 = System.nanoTime();
				CouchDB.search(ServerName, dbName, searchString);
				long time2 = System.nanoTime();
				double totTime = (time2-time1)/1000000000.0;
				System.out.println("Searching at "+(i+1)+" run(s) took "+totTime+" seconds");
				MapResults.add(totTime);
			}
			long endtime = System.nanoTime();
			double LongTime = (endtime-starttime)/1000000000.0;
			System.out.println("Time taken to complete all the "+NoOfRuns+" runs is "+LongTime+" seconds\n");
		}
	}
}
