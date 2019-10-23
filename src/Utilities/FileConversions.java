package Utilities;

import java.io.BufferedReader;
import java.io.FileReader;

public class FileConversions {


	public static String readFileAsString(String Path) throws java.io.IOException
	{
	    BufferedReader br = new BufferedReader(new FileReader(Path));
	    String readLine, result = "";
	    while(( readLine = br.readLine()) != null)
	    {
	        result += readLine;
	    }
	    br.close();
	    return result;
	}
}
