package aconex.customer1800;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Helpers {
	public static Map<Character,Character> buildLetter2DigitsMap(){
		Map<Character,Character> letter2DigitMap = new HashMap<>();
		char digit = '2';
		int i = 1;
		for(char c = 'A'; c <= 'O'; c++){
			letter2DigitMap.put(c, digit);
			if(i%3==0)
				digit++;
			i++;
		}
		for(char c = 'P'; c <= 'S'; c++){
			letter2DigitMap.put(c, digit);
		}
		digit++;
		for(char c = 'T'; c <= 'V'; c++){
			letter2DigitMap.put(c, digit);
		}
		digit++;
		for(char c = 'W'; c <= 'Z'; c++){
			letter2DigitMap.put(c, digit);
		}
		return letter2DigitMap;
	}
	public static Set<String> buildWordSet(File file) throws IOException {	
		Set<String> words = new HashSet<>();
		FileInputStream fstream = new FileInputStream(file.getAbsolutePath());
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String line;
		line = br.readLine();
		while (line != null && !line.isEmpty())   {
			words.add(line.trim().toUpperCase());
			line = br.readLine();
		}
		br.close();
		fstream.close();
		return words;
	}
	
	public static List<String> generatePerms(String readString, Map<Character, Character> letter2DigitsMap, Map<String, String> words) {
		DataStructures dataStructures = new DataStructures();
		List<String> numbers = dataStructures.wordBreak(readString, words.keySet());
		List<String> res = mapBack(numbers, words);
		return res;
	}
	private static List<String> mapBack(List<String> numbers, Map<String, String> words) {
		List<String> res = new ArrayList<>();
		for(String s: numbers){
			StringBuffer sb = new StringBuffer();
			String[] arr = s.split("-");
			for(String a: arr)
				sb.append(words.get(a)).append('-');
			res.add(sb.toString().substring(0, sb.toString().length()-1));
		}
		return res;
	}
	public static void display(Collection<String> res) {
		for(String s: res)
			System.out.println(s);
	}
	public static Map<String, String> buildWordMap(File dictFile, Map<Character, Character> letter2DigitsMap) throws IOException {
		Map<String, String> words = new HashMap<>();
		FileInputStream fstream = new FileInputStream(dictFile.getAbsolutePath());
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String line;
		line = br.readLine();
		while (line != null && !line.isEmpty())   {
			StringBuffer number = new StringBuffer();
			char[] word = line.trim().toUpperCase().toCharArray();
			for(char c: word){
				number.append(letter2DigitsMap.get(c));
			}
			words.put(number.toString(), line.trim().toUpperCase());
			line = br.readLine();
		}
		br.close();
		fstream.close();
		return words;
	}
	public static String extractNumbers(String trim) {
		// TODO Auto-generated method stub
		StringBuffer s = new StringBuffer();
		for(char c:trim.toCharArray())
			if(c>='0'&&c<='9')
				s.append(c);
		return s.toString();
	}
}
