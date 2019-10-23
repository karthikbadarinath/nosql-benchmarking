/**
 *
 */
package Benchmark;

import java.util.ArrayList;

import Benchmark.ReadUpdateThread;

/**
 * @author Karthik Badarinath
 *
 */
public class CouchDBBenchmark {

	public static int ReadErrors = 0;
	public static int UpdateErrors = 0;
	public static ArrayList<Double> FinalResult;

	public static void benchmark(String dbName, int NoOfOperations, String[] IPAddress, int ReadPercentage, int NoOfDoc){

		int NoOfNode = IPAddress.length;
		if(NoOfNode > 0){
			FinalResult = new ArrayList<Double>();
			long time1 = System.nanoTime();
			int RunCount = 1;
			do{
				FinalResult.add(RunBenchmark(dbName, NoOfOperations, IPAddress, ReadPercentage, NoOfDoc));
				RunCount++;
			}while(RunCount <= 10);
			long time2 = System.nanoTime();
			double time = (time2-time1)/1000000000.0;
			System.out.println("\nTime taken to complete all the reads is: " + time + "seconds");
			ArrayList<Double> timeinseconds = new ArrayList<Double>();
			for(int k = 0;k<FinalResult.size();k++){
				timeinseconds.add(k,FinalResult.get(k)/1000000000.0);
			}
			System.out.println("Individual Results in seconds: "+timeinseconds);
			System.out.print("\n");
		}
	}

	public static double RunBenchmark(String dbName, int NoOfOperations, String[] iPAddress2, int ReadPercentage, int NoOfDoc){
		int OperationsPerThread = NoOfOperations/iPAddress2.length;
		ReadUpdateThread thread[] = new ReadUpdateThread[iPAddress2.length];
		long time1 = System.nanoTime();
		for(int i=0;i<iPAddress2.length;i++){
			System.out.println("\nBeginning READ benchmark from thread no "+(i+1));
			thread[i] = new ReadUpdateThread(dbName, OperationsPerThread, iPAddress2[i], ReadPercentage, NoOfDoc);
			thread[i].start();
		}

		// After completing all the operations
		for(int j=0;j<iPAddress2.length;j++){
			try{
				thread[j].join();
			} catch(InterruptedException ex){
				System.out.println("Thread exception found...\n" +ex.getMessage());
			}
		}

		long time2 = System.nanoTime();
		long longtime = time2-time1;
		double timeinseconds = longtime/1000000000.0;
		System.out.println("Time taken to complete " + NoOfOperations + " operations is " + timeinseconds + " seconds");
		return longtime;
	}

}
