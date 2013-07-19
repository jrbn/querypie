package nl.vu.cs.querypie.dictionary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class DictionarySorter {
	
	private static class Index implements Comparable<Index> { 
		long hash;
		long offset;
		int size;
		
		@Override
		public int compareTo(Index o) {
		
			if (hash < o.hash) { 
				return -1;
			}
			
			if (hash > o.hash) { 
				return 1;
			}
			
			return 0;
		}
	}
	
	private static void sort(String dir) throws IOException { 

		File f = new File(dir + File.separator + "index.data");
		
		long count = f.length() / (8+8+4);
		
		System.out.println("File contains " + count + " entries");
		
		System.out.println("Creating array[" + count + "]");
		
		Index [] data = new Index[(int)count];
		
		System.out.println("Reading data");
		
		long start = System.currentTimeMillis();
		
		DataInputStream in = new DataInputStream(
				new BufferedInputStream(
						new FileInputStream(f)));

		for (int i=0;i<count;i++) { 
	
			Index tmp = new Index();
			tmp.hash = in.readLong();
			tmp.offset = in.readLong();
			tmp.size = in.readInt();
			
			data[i] = tmp;
			
			if (i % 1000000 == 0) { 
				System.out.println("Read " + i);
			}
			
		}
		
		in.close();

		long end = System.currentTimeMillis();
		
		System.out.println("Read " + count + " entries " + (end-start));
		
		System.out.println("Sorting");

		start = System.currentTimeMillis();
		
		Arrays.sort(data);
		
		end = System.currentTimeMillis();
		
		System.out.println("Done sorting " + (end-start));
		
		System.out.println("Writing");
		
		start = System.currentTimeMillis();
		
		DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(
				new FileOutputStream(dir + File.separator + "index_sorted.data")));

		for (int i=0;i<count;i++) { 
	
			Index tmp = data[i];
			
			out.writeLong(tmp.hash);
			out.writeLong(tmp.offset);
			out.writeInt(tmp.size);
		}
		
		out.close();
		
		end = System.currentTimeMillis();
		
		System.out.println("Done " + (end-start));
	}

	public static void main(String[] args) throws IOException {

		// Load the dictionary and serve requests
		System.out.println("Loading dictionary ...");
		sort(args[0]);
		System.out.println("done ...");
	}
}
