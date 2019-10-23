package Implementation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import net.sf.json.JSONException;

import com.fourspaces.couchdb.Database;
import com.fourspaces.couchdb.Document;
import com.fourspaces.couchdb.Session;

/**
 *This is the class that helps perform operations in other classes of every package
 *This class performs all the operartions to the database
 **/

/**
 * @author Karthik Badarinath
 *
 */
@SuppressWarnings("deprecation")
public class CouchDB {

	static Session dbCon;
	static Database db;
	static Document doc;

	public static Session session(String ServerAddress){
		dbCon = new Session(ServerAddress, 5984);
		return dbCon;
	}

	public static void createDB(String dbName){

		// Creating a database in CouchDB
		try{
			db = dbCon.getDatabase(dbName);
			System.out.println("\nUnable to create "+dbName.toUpperCase()+". Database already exists...\n");
		}catch(Exception e){
			if(db == null){
				dbCon.createDatabase(dbName);
				System.out.println("\n" + dbName.toUpperCase() + "" + " database now CREATED successfully...\n");
			}
		}
	}

	public static String read(Session dbCon, String dbName, String ID){
		String data = null;
		try{
			db = dbCon.getDatabase(dbName);
			doc = db.getDocument(ID);
			data = doc.toString();
			}catch(JSONException json){
				System.out.println("JSON Exception, Something went wrong while reading the document ");
				System.out.println("Please check if ID number "+ID+" exists...");
			}
		return data;
	}

	public static int writeDB(String ID, String dbName, String xml) {
		// Adding data to the database
		int retValue = 1;
		try{
			db = dbCon.getDatabase(dbName);
			doc = new Document();
			doc.setId(ID);
			doc.put("Article", xml);
			db.saveDocument(doc);
		} catch(JSONException ex){
			if(db == null){
				System.out.println("Database does not exists!");
				retValue = -1;
			}
			else{
				System.out.println("JSON Exception, Something went wrong while writing the document.");
				System.out.println("Please check if ID number "+ID+" already exists...");
				retValue = -1;
			}
		}
		return retValue;
	}

	public static int update(Session dbCon, String dbName, String document, String ID){
		try{
			String newdoc = document+"1";
			db = dbCon.getDatabase(dbName);
			doc = db.getDocument(ID);
			doc.put("Article", newdoc);
			db.saveDocument(doc);
		}catch(JSONException json){
			System.out.println("JSON Exception, Something went wrong while updating the document");
		}
		return 1;
	}

	public static int addView(String dbName, String KeyWord){
		int catchex = 0, ret = 0;
		try{
			db = dbCon.getDatabase(dbName);
			doc = db.getDocument("_design/"+KeyWord);
		} catch(Exception ex){
			catchex = 1;
		}
		try{
			if(catchex == 1){
				doc = new Document();
				doc.setId("_design/"+KeyWord);
				doc.put("language", "javascript");
				String str = "{\"mapview\": {\"map\": \"function(doc) { var artValue = doc.Article.split(' '); var keyword = '"+ KeyWord +"'; for (var i in artValue) { if (artValue[i].trim() == keyword)  emit(doc._id, 1); } } \", \"reduce\": \"function(key, value, rereduce){ return sum(value) } \"} }";
				doc.put("views", str);
				db.saveDocument(doc);
				System.out.println("\nPermanent view created...\nPlease wait while the benchmarking has started...");
			}
			else{
				System.out.println("You have already queried this search, this software creates a permanent view for every search inside CouchDB for various reasons...\nPlease try a different string search to continue...");
				ret = -1;
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		return ret;
	}

	public static void search(String ServerName, String dbName, String designDocumentName){
		org.apache.http.client.HttpClient httpclient = new DefaultHttpClient();

		HttpGet get = new HttpGet("http://"+ServerName+":5984/"+dbName+"/_design/"+designDocumentName+"/_view/mapview");
		HttpResponse response = null;
		try {
			response = httpclient.execute(get);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HttpEntity entity=response.getEntity();
		InputStream instream = null;
		try {
			instream = entity.getContent();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(instream));
		@SuppressWarnings("unused")
		String strdata = null;

		try {
			while( (strdata =reader.readLine())!=null)
			{
			       //System.out.println(strdata);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
