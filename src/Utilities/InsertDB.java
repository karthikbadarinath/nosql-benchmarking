/**
 * This class is only used to fill the Couch Database
 */
package Utilities;

import java.io.FileNotFoundException;
import java.io.IOException;

import net.sf.json.JSONException;

import Implementation.CouchDB;

/**
 * @author Karthik
 *
 */
public class InsertDB {

	static int failed = 1, count = 1;

	public static void insert(int NoOfInsertRuns, int NoOfInserts, int StartIDinDB, int ArtStartNo, String dPath, String dbName){

			// Declaration
			int i = 0, j, retValue;
			float time1 = 0, time2 = 0, starttime = 0, endtime = 0, time = 0, totaltime = 0;

			// Insert operations
			System.out.println("\nPlease wait while the insertion operation is taking place...");
			time1 = System.nanoTime();
			starttime = System.nanoTime();
			for (j=1;j<=NoOfInsertRuns;j++) {
				for (i=0;i<NoOfInserts;i++) {
					String xml;
					try{
						xml = FileConversions.readFileAsString(dPath + String.valueOf(i + ArtStartNo));
						retValue = CouchDB.writeDB(String.valueOf(StartIDinDB+i), dbName, xml);
						if(retValue == 1){
							count++;
						}
						else{
							i = NoOfInserts + 1;
						}
					}catch(FileNotFoundException fne){
						System.out.println("Sorry, the provided file or directory is not found!");
						break;
					}catch (IOException e) {
						System.out.println("Insert of article no "+(StartIDinDB+i)+" failed");
						failed++;
					}catch(JSONException json){
						System.out.println("Sorry! Something went wrong, please try again...\n");
						break;
					}

					if((i+1)%100 == 0){
						time2 = System.nanoTime();
						time = (time2-time1)/1000000000;
						System.out.println((count-1)-(failed-1) +" articles inserted ("+ time +" seconds!)");
						time1 = System.nanoTime();
					}
				}
				StartIDinDB += NoOfInserts;
			}
			endtime = System.nanoTime();
			totaltime = (endtime-starttime)/1000000000;
			System.out.println("\nInsertion of "+ (count-1) +" articles complete in " + totaltime +" seconds");
			System.out.println(failed-1 +" articles failed to get inserted!\n");
		}

}
