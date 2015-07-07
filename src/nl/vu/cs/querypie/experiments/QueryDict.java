package nl.vu.cs.querypie.experiments;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.querypie.Query;

public class QueryDict {

    public static void main(String[] args) throws Exception {
	if (args.length == 0 || args[0].equals("--help")) {
	    System.out.println("Usage: term [-d dictionaryHost]");
	    return;
	}

	String term = args[0];
	Query q = new Query();
	q.dictionaryHost = "localhost";

	TLong[] req = new TLong[1];
	req[0] = new TLong();
	req[0].setValue(Long.valueOf(term));
	String[] res = q.getText(req);
	for (String r : res) {
	    System.out.println(r);
	}
    }
}
