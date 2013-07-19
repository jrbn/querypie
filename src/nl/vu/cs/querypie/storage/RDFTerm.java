package nl.vu.cs.querypie.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.data.types.DataProvider;
import arch.data.types.SimpleData;

public final class RDFTerm extends SimpleData {

    public static final int DATATYPE = 6;

    static {
	DataProvider.addType(DATATYPE, RDFTerm.class);
    }

    @Override
    public boolean equals(SimpleData a, ActionContext context) {

	if (a.getIdDatatype() != getIdDatatype()) {
	    return false;
	}

	if (((RDFTerm) a).value == this.value) {
	    return true;
	} else if (((RDFTerm) a).value != -1 && value == -1) {
	    return true;
	} else if (value <= THRESHOLD_VARIABLE) {
	    Collection<Long> set2 = Schema.getInstance().getCustomSubset(
		    value, context);

	    if (((RDFTerm) a).value <= THRESHOLD_VARIABLE) {
		Collection<Long> set1 = Schema.getInstance().getCustomSubset(
			((RDFTerm) a).getValue(), context);
		if (set1 instanceof TreeSet && set2 instanceof TreeSet) {
		    if (set1.size() != set2.size()) {
		        return false;
		    }
		    Iterator<Long> i1 = set1.iterator();
		    Iterator<Long> i2 = set2.iterator();
		    while (i1.hasNext()) {
		        if (i1.next().longValue() != i2.next().longValue()) {
		            return false;
		        }
		    }
		    return true;
		}
		return set1.equals(set2);
	    } else if (((RDFTerm) a).getValue() >= 0) {
		return set2.contains(((RDFTerm) a).getValue());
	    }

	}

	return false;
    }

    public static final int THRESHOLD_VARIABLE = -100;
    static final Logger log = LoggerFactory.getLogger(RDFTerm.class);

    long value;
    private String name;

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public boolean equals(RDFTerm term) {
	return value == term.value;
    }

    @Override
    public boolean equals(Object term) {
	log.error("This should never happen! "
		+ Thread.currentThread().getStackTrace()[2]);
	return false;
    }

    public void setValue(long value) {
	this.value = value;
    }

    public long getValue() {
	return value;
    }

    public RDFTerm() {

    }

    public RDFTerm(long value) {
	this.value = value;
    }

    public boolean isFixed() {
	return value >= 0;
    }

    public boolean isUnknown() {
	return value == -1;
    }

    public boolean isSet() {
	return value <= Schema.SET_THRESHOLD;
    }

    @Override
    public int getIdDatatype() {
	return DATATYPE;
    }

    @Override
    public void readFrom(DataInput input) throws IOException {
	value = input.readLong();
    }

    @Override
    public void writeTo(DataOutput output) throws IOException {
	output.writeLong(value);
    }

    @Override
    public int bytesToStore() {
	return 8;
    }

    @Override
    public String toString() {
	return Long.toString(value);
    }
}