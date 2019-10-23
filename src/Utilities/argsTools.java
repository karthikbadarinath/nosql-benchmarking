package Utilities;

import java.util.ArrayList;

public class argsTools {

	public static ArrayList<ArrayList<String>> divideNodeList(String[] nodeList, int numberOfClients){

		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();

		int numberOfNodes = nodeList.length;
		int low=0;
		int up=numberOfNodes/numberOfClients;

		for(int i=0;i<numberOfClients;i++){
			ArrayList<String> local = new ArrayList<String>();
			for(int j=low;j<up;j++){
				if(j<nodeList.length)
					local.add(nodeList[j]);
				if(j==(nodeList.length-(numberOfNodes%numberOfClients)-1)){
					for(int k=1;k<=(numberOfNodes%numberOfClients);k++){
						local.add(nodeList[j+k]);
					}
				}
			}
			result.add(local);
			low = up;
			up += numberOfNodes/numberOfClients;
		}
		return result;
	}

}
