package nl.vu.cs.querypie.reasoner.rules;

import java.util.Set;

import nl.vu.cs.querypie.reasoner.Pattern;
import nl.vu.cs.querypie.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Rule {

    static final Logger log = LoggerFactory.getLogger(Rule.class);

    public int id;
    public int type;
    public Pattern HEAD;
    public boolean isActive;

    public Set<Long>[] head_excludedValues;

    public Rule(int id, String head) {
	// All rules are active
	isActive = true;
	HEAD = Utils.parsePattern(head);
	this.id = id;
    }
}