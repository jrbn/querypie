package nl.vu.cs.querypie.reasoner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.Rule3;
import nl.vu.cs.querypie.reasoner.rules.Rule4;
import nl.vu.cs.querypie.storage.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Ruleset {

    static final public String RULES_FILE = "rules.list";
    static final public String RULES_FILE_AFTER_CLOSURE = "rules.list_after_closure";

    static final Logger log = LoggerFactory.getLogger(Ruleset.class);

    private static Ruleset ruleset = new Ruleset();
    private String[] sec_heads = null;
    private String[][] sec_precomps = null;
    private String[][] sec_generics = null;
    private String[][] sec_locations = null;
    private String[] sec_list_heads = null;
    private String[] sec_list_loc = null;
    private int[] types = null;

    private List<Rule1> rules1;
    private List<Rule2> rules2;
    private List<Rule3> rules3;
    private List<Rule4> rules4;

    private Rule1[] arules1;
    private Rule2[] arules2;
    private Rule3[] arules3;
    private Rule4[] arules4;

    private List<Rule1> activeInactiveRules;

    public void parseRulesetFile(String rulesFile) throws IOException {
	log.info("Start parsing the ruleset file");

	List<String> sec_h = new ArrayList<String>();
	List<String[]> sec_g = new ArrayList<String[]>();
	List<String[]> sec_p = new ArrayList<String[]>();
	List<String[]> sec_l = new ArrayList<String[]>();

	List<String> sec_list_var = new ArrayList<String>();
	List<String> sec_list_loc = new ArrayList<String>();

	List<Integer> t = new ArrayList<Integer>();

	BufferedReader f = new BufferedReader(new FileReader(
		new File(rulesFile)));
	String line = f.readLine();
	while (line != null) {
	    String[] type = line.split("\t");
	    int typeRule = Integer.valueOf(type[0]);
	    t.add(typeRule);

	    String signature = type[1];
	    String[] split = signature.split(" :- ");
	    sec_h.add(split[0].substring(1, split[0].length() - 1));

	    if (typeRule == 1) {
		String[] patterns = split[1].split(",");
		// Parse the precom patterns
		String[] sign = new String[patterns.length];
		String[] loc = new String[patterns.length];
		for (int i = 0; i < patterns.length; ++i) {
		    String[] p = patterns[i].split("@");
		    sign[i] = p[0].substring(1, p[0].length() - 1);
		    loc[i] = p[1].substring(1, p[1].length() - 1);
		}
		sec_p.add(sign);
		sec_l.add(loc);

		// There are no generic patterns
		sec_g.add(null);

		// No lists
		sec_list_var.add(null);
		sec_list_loc.add(null);
	    } else if (typeRule == 2) {
		String[] patterns = split[1].split(",");
		// Parse the precom patterns
		String[] sign = new String[patterns.length - 1];
		String[] loc = new String[patterns.length - 1];
		for (int i = 0; i < patterns.length - 1; ++i) {
		    String[] p = patterns[i].split("@");
		    sign[i] = p[0].substring(1, p[0].length() - 1);
		    loc[i] = p[1].substring(1, p[1].length() - 1);
		}
		sec_p.add(sign);
		sec_l.add(loc);

		// Parse the generic patterns
		String[] g = new String[1];
		g[0] = patterns[patterns.length - 1].substring(1,
			patterns[patterns.length - 1].length() - 1);
		sec_g.add(g);

		// No lists
		sec_list_var.add(null);
		sec_list_loc.add(null);
	    } else if (typeRule == 3) {

		String[] mainsplit = split[1].split("@@");

		// Parse the precom patterns
		String[] patterns = mainsplit[0].split(",");
		String[] sign = new String[patterns.length];
		String[] loc = new String[patterns.length];
		for (int i = 0; i < patterns.length; ++i) {
		    String[] p = patterns[i].split("@");
		    sign[i] = p[0].substring(1, p[0].length() - 1);
		    loc[i] = p[1].substring(1, p[1].length() - 1);
		}
		sec_p.add(sign);
		sec_l.add(loc);

		// Parse the generic patterns
		patterns = mainsplit[1].split(",");
		String[] p = new String[patterns.length];
		int i = 0;
		for (String pattern : patterns) {
		    p[i++] = pattern.substring(1, pattern.length() - 1);
		}
		sec_g.add(p);

		// No lists
		sec_list_var.add(null);
		sec_list_loc.add(null);
	    } else if (typeRule == 4) {
		String[] mainsplit = split[1].split("@@");

		// Parse the precom patterns
		String[] patterns = mainsplit[0].split(",");
		String[] sign = new String[patterns.length];
		String[] loc = new String[patterns.length];
		for (int i = 0; i < patterns.length; ++i) {
		    String[] p = patterns[i].split("@");
		    sign[i] = p[0].substring(1, p[0].length() - 1);
		    loc[i] = p[1].substring(1, p[1].length() - 1);
		}
		sec_p.add(sign);
		sec_l.add(loc);

		// Parse the list
		String[] head_details = mainsplit[1].split("@");
		String head_list = head_details[0].substring(2,
			head_details[0].length() - 1);
		String head_loc = head_details[1].substring(1,
			head_details[1].length() - 1);
		sec_list_var.add(head_list);
		sec_list_loc.add(head_loc);

		// Parse the generic patterns
		if (mainsplit.length == 3) {
		    patterns = mainsplit[2].split(",");
		    String[] p = new String[patterns.length];
		    int i = 0;
		    for (String pattern : patterns) {
			p[i++] = pattern.substring(1, pattern.length() - 1);
		    }
		    sec_g.add(p);
		} else {
		    sec_g.add(null);
		}
	    }

	    line = f.readLine();
	}

	f.close();
	
	sec_heads = sec_h.toArray(new String[sec_h.size()]);
	sec_precomps = sec_p.toArray(new String[sec_h.size()][]);
	sec_generics = sec_g.toArray(new String[sec_h.size()][]);
	sec_locations = sec_l.toArray(new String[sec_h.size()][]);
	sec_list_heads = sec_list_var.toArray(new String[sec_list_var.size()]);
	this.sec_list_loc = sec_list_loc
		.toArray(new String[sec_list_loc.size()]);
	types = new int[t.size()];
	for (int i = 0; i < t.size(); ++i) {
	    types[i] = t.get(i);
	}
    }

    public void loadRules(boolean cleanAll) throws Exception {
	log.info("Start loading the rules ...");

	Schema.getInstance().clear(cleanAll);

	rules1 = new ArrayList<Rule1>();
	rules2 = new ArrayList<Rule2>();
	rules3 = new ArrayList<Rule3>();
	rules4 = new ArrayList<Rule4>();

	activeInactiveRules = new ArrayList<Rule1>();
	
	for (int i = 0; i < types.length; ++i) {
	    long time = System.currentTimeMillis();

	    if (types[i] == 1) {
		Rule1 rule = new Rule1(rules1.size(), sec_heads[i],
			sec_precomps[i], sec_locations[i]);
		if (rule.isActive) {
		    rules1.add(rule);
		}
		activeInactiveRules.add(rule);
	    } else if (types[i] == 2) {
		Rule2 rule = new Rule2(rules2.size(), sec_heads[i],
			sec_precomps[i], sec_locations[i], sec_generics[i]);
		if (rule.isActive) {
		    rules2.add(rule);
		}
		activeInactiveRules.add(rule);
	    } else if (types[i] == 3) {
		Rule3 rule = new Rule3(rules3.size(), sec_heads[i],
			sec_precomps[i], sec_locations[i], sec_generics[i]);
		if (rule.isActive) {
		    rules3.add(rule);
		}
		activeInactiveRules.add(rule);
	    } else if (types[i] == 4) {
		Rule4 rule = new Rule4(rules4.size(), sec_heads[i],
			sec_precomps[i], sec_locations[i], sec_list_heads[i],
			sec_list_loc[i], sec_generics[i]);
		if (rule.isActive) {
		    rules4.add(rule);
		}
		activeInactiveRules.add(rule);
	    } else {
		throw new Exception("Type not recognized");
	    }

	    log.debug("Loaded rule "
		    + activeInactiveRules.get(activeInactiveRules.size() - 1).id
		    + " of type " + types[i] + " in "
		    + (System.currentTimeMillis() - time));
	}

	// log.info("Freeing memory");
	// Schema2.getInstance().freeMemory();

	log.debug("Run rules optimizer ...");
	new RulesOptimizer().runOptimizer(rules1, rules2, rules3);

	arules1 = rules1.toArray(new Rule1[rules1.size()]);
	arules2 = rules2.toArray(new Rule2[rules2.size()]);
	arules3 = rules3.toArray(new Rule3[rules3.size()]);
	arules4 = rules4.toArray(new Rule4[rules4.size()]);

	log.info("Finished loading the ruleset. Memtotal: "
		+ Runtime.getRuntime().totalMemory() + " free mem: "
		+ Runtime.getRuntime().freeMemory());
    }

    private Ruleset() {
	try {
	    parseRulesetFile(System.getProperty(RULES_FILE));
	    loadRules(true);
	} catch (Exception e) {
	    log.error("Error loading the ruleset", e);
	}
    }

    public static Ruleset getInstance() {
	return ruleset;
    }

    public Rule1[] getAllActiveFirstTypeRules() {
	return arules1;
    }

    public Rule2[] getAllActiveSecondTypeRules() {
	return arules2;
    }

    public Rule3[] getAllActiveThirdTypeRules() {
	return arules3;
    }

    public Rule4[] getAllActiveFourthTypeRules() {
	return arules4;
    }

    public Rule1 getRuleFirstType(int index) {
	return rules1.get(index);
    }

    public Rule2 getRuleSecondType(int index) {
	return rules2.get(index);
    }

    public Rule3 getRuleThirdType(int index) {
	return rules3.get(index);
    }

    public Rule4 getRuleFourthType(int index) {
	return rules4.get(index);
    }

    public Pattern[] getPrecomputedPatterns() {
	Set<Pattern> listPatterns = new HashSet<Pattern>();

	for (Rule1 rule : activeInactiveRules) {
	    if (rule.PRECOMPS != null) {
		for (Pattern p : rule.PRECOMPS)
		    listPatterns.add(p);
	    }
	}

	return listPatterns.toArray(new Pattern[listPatterns.size()]);
    }
}