package nl.vu.cs.querypie.sparql;

import org.openrdf.model.Value;

public class SesameValue implements Value {

	private static final long serialVersionUID = -1317779371039463833L;

	public long value;

	public SesameValue(long value) {
		this.value = value;
	}

	@Override
	public String stringValue() {
		return Long.toString(value);
	}

}
