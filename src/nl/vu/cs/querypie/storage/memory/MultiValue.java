package nl.vu.cs.querypie.storage.memory;

import java.util.Arrays;

public class MultiValue {

    public long[] values;

    public MultiValue(long... values) {
	this.values = values;
    }

    public MultiValue(long[] bindings, int x, int sizeTuple) {
        values = new long[sizeTuple];
	System.arraycopy(bindings, x, values, 0, sizeTuple);
    }

    public void setNumberFields(int length) {
	if (values == null || values.length != length)
	    values = new long[length];
    }

    @Override
    public boolean equals(Object obj) {
	if (!(obj instanceof MultiValue))
	    return false;

	return Arrays.equals(((MultiValue) obj).values, values);
    }

    @Override
    public int hashCode() {
	return Arrays.hashCode(values);
    }

    public long[] getRawFields() {
	return values;
    }

    public String toString() {
	return Arrays.toString(values);
    }

    public static class Comparator implements java.util.Comparator<MultiValue> {

	@Override
	public int compare(MultiValue o1, MultiValue o2) {
	    return MultiValue.compare(o1.values, 0, o2.values, 0,
		    o1.values.length);
	}
    }

    public static int compare(long[] v1, int start1, long[] v2, int start2,
	    int length) {
	for (int i = 0; i < length; ++i) {
	    if (v1[start1] < v2[start2]) {
		return -1;
	    } else if (v1[start1] > v2[start2]) {
		return 1;
	    }

	    start1++;
	    start2++;
	}

	return 0;
    }

    public MultiValue newCopy() {
	long[] newValues = Arrays.copyOf(values, values.length);
	return new MultiValue(newValues);
    }
}
