package nl.vu.cs.querypie.dictionary;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

public class DictionaryTestSorted {

	private static void test(String file) throws IOException { 
		
		DataInputStream in = new DataInputStream(
				new BufferedInputStream(
			    new FileInputStream(file)));
		
		// Read first entry
		long firstIndex = in.readLong(); 
		in.readLong();
		in.readInt();
	
		long count = 1;
		
		boolean done = false;
		
		while (!done) {
			try { 
				long index = in.readLong(); 
				in.readLong();
				in.readInt();

				count++;
				
				if (index < firstIndex) { 
					System.out.println("NOT sorted!" + firstIndex + " " + index);
				}
				
				firstIndex = index;
			
			} catch (EOFException e) {
				done = true;
			}
		}
		
		in.close();
		
		System.out.println("Done " + count);
	}
	
	public static void main(String[] args) throws IOException {
	
		// Load the dictionary and serve requests
		System.out.println("Loading dictionary ...");
		test(args[0]);
		System.out.println("done ...");
	}
	
}
