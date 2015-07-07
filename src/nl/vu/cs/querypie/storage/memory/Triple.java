package nl.vu.cs.querypie.storage.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;

public class Triple extends SimpleData implements Comparable<Triple>,
        Serializable {

    private static final long serialVersionUID = 5837200028147380236L;

    public static int DATATYPE = DataProvider.addType(Triple.class);

    public long subject, predicate, object;

    @Override
    public void readFrom(DataInput input) throws IOException {
        subject = input.readLong();
        predicate = input.readLong();
        object = input.readLong();
    }

    @Override
    public void writeTo(DataOutput output) throws IOException {
        output.writeLong(subject);
        output.writeLong(predicate);
        output.writeLong(object);
    }

    @Override
    public boolean equals(Object arg0) {
        if (!(arg0 instanceof Triple)) {
            return false;
        }
        return equals((Triple) arg0);
    }

    @Override
    public boolean equals(SimpleData o, ActionContext context) {
        if (!(o instanceof Triple)) {
            return false;
        }
        return equals((Triple) o);
    }

    public boolean equals(Triple o) {
        return subject == o.subject && predicate == o.predicate
                && object == o.object;
    }

    @Override
    public int hashCode() {
        long l = subject + predicate + object;
        return ((int) (l >>> 32) ^ (int) l);
    }

    @Override
    public int getIdDatatype() {
        return DATATYPE;
    }

    @Override
    public String toString() {
        return subject + " " + predicate + " " + object;
    }

    @Override
    public int compareTo(SimpleData obj) {
        Triple o = (Triple) obj;
        return compareTo(o);
    }

    @Override
    public int compareTo(Triple o) {
        if (subject < o.subject)
            return -1;
        else if (subject > o.subject)
            return 1;
        else if (predicate < o.predicate)
            return -1;
        else if (predicate > o.predicate)
            return 1;
        else if (object < o.object)
            return -1;
        else if (object > o.object)
            return 1;
        else
            return 0;
    }

    @Override
    public void copyTo(SimpleData el) {
        Triple o = (Triple) el;
        o.subject = subject;
        o.predicate = predicate;
        o.object = object;
    }
}
