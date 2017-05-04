package aconex.customer1800;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App 
{
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	private static File dictFile;
	private static File numbersFile;
	private static Map<Character, Character> letter2DigitsMap;
	private static Map<String, String> words;
	public static void main(String args[]) throws IOException{
		LOGGER.setLevel(Level.INFO);
		App customer1800 = new App();
		if(!customer1800.buildArgs(args)){
			LOGGER.info("Usage : -d <path to dictonary> <optional path to numbers>");
			System.exit(0);
		}
		customer1800.setDataStructures();
		customer1800.readInputStream();	
	}
	
	public void setDataStructures() throws IOException {
		letter2DigitsMap = Helpers.buildLetter2DigitsMap();
		words = Helpers.buildWordMap(dictFile, letter2DigitsMap);
	}

	private void readInputStream() throws FileNotFoundException{
		Scanner scanner;
		if(numbersFile!=null)
			scanner = new Scanner(numbersFile);
		else
			scanner = new Scanner(System.in);
		String readString;
		while(scanner.hasNextLine()) {
			readString = scanner.nextLine();
			if (readString.equals(""))
			       break;
			
			//test this
			List<String> res = generatePerms(readString);
			Helpers.display(res);
		}
		scanner.close();
	}

	public List<String> generatePerms(String readString){
		String justNumbers = Helpers.extractNumbers(readString.trim());
		return Helpers.generatePerms(justNumbers, letter2DigitsMap, words);
	}

	public boolean buildArgs(String[] args) {
		int i=0;
		int dictIdx = -1;
		while( i < args.length ){
		    String a = args[i];
		    if(a.equals("-d")){
		    	dictIdx = i;
		    	i = i + 1;
		    }
		    else{ 
		    	numbersFile =  new File(args[i]);
		    	//invalid numbers file
			    if( !numbersFile.exists() || numbersFile.isDirectory() )
		    		return false;
		    }
		    i = i + 1;
		}
		//no argument provided, we need at least the dictionary argument
		if( i == 0 )
			return false;
		//no dictionary option in argument
		if( dictIdx == -1)
			return false;
		//no dictionary file provided
		if( dictIdx+1 == args.length ){
    		return false;
    	}
    	dictFile = new File(args[dictIdx+1]);
    	//invalid dictionary file provided
    	if( !dictFile.exists() || dictFile.isDirectory() )
    		return false;
    	//all good
		return true;
	}
}
