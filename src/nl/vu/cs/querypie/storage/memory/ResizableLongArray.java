package nl.vu.cs.querypie.storage.memory;

import java.util.Arrays;

public class ResizableLongArray {
    
    public long[] array = new long[1024];
    public int currentSize = 0;

    public void add(long[] input) {
	if (array.length < currentSize + input.length) {
	    //Increment it up to 30% or the size of the input value
	    int increment = Math.max(array.length / 3, input.length);
	    long[] newArray = new long[array.length + increment];
	    System.arraycopy(array, 0, newArray, 0, currentSize);
	    array = newArray;
	}
	System.arraycopy(input, 0, array, currentSize, input.length);
	currentSize += input.length;
    }

    public void addAll(ResizableLongArray input) {
	if (array.length < currentSize + input.currentSize) {
	    //Increment it up to 30% or the size of the input value
	    int increment = Math.max(array.length / 3, input.currentSize);
	    long[] newArray = new long[array.length + increment];
	    System.arraycopy(array, 0, newArray, 0, currentSize);
	    array = newArray;
	}
	System.arraycopy(input.array, 0, array, currentSize, input.currentSize);
	currentSize += input.currentSize;
    }

    public long[] getLongArray() {
	return Arrays.copyOf(array, currentSize);
    }    
}