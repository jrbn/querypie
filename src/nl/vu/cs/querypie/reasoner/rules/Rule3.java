package nl.vu.cs.querypie.reasoner.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule3 extends Rule2 {

    static final Logger log = LoggerFactory.getLogger(Rule3.class);
    
    public Rule3(int id, String head, String[] precomps,
	    String[] locations) throws Exception {
	super(id, head, precomps, locations);
	type = 3;
    }
    
    public Rule3(int id, String head, String[] precomps,
	    String[] locations, String[] generics) throws Exception {
	super(id, head, precomps, locations, generics);
	type = 3;
    }
}