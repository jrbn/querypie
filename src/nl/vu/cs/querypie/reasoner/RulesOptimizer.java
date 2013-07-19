package nl.vu.cs.querypie.reasoner;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.querypie.reasoner.rules.Rule1;
import nl.vu.cs.querypie.reasoner.rules.Rule2;
import nl.vu.cs.querypie.reasoner.rules.Rule3;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RulesOptimizer {

    static final Logger log = LoggerFactory.getLogger(RulesOptimizer.class);

    public void runOptimizer(List<Rule1> rules1,
	    List<Rule2> rules2, List<Rule3> rules3) throws Exception {

	List<Rule1> rules = new ArrayList<Rule1>();
	rules.addAll(rules1);
	rules.addAll(rules2);
	rules.addAll(rules3);

	for (Rule1 rule : rules) {
	    
	    if (!rule.isActive)
		    continue;

	    @SuppressWarnings("unchecked")
	    Set<Long>[] excludedValues = (Set<Long>[]) Array.newInstance(
		    HashSet.class, 3);
	    for (int i = 0; i < 3; ++i) {
		excludedValues[i] = new HashSet<Long>();
	    }

	    // Check against all other rules
	    for (Rule1 other_rule : rules) {
		
		if (!other_rule.isActive)
		    continue;

		if (rule.type != other_rule.type) {
		    // We start by looking if two rules have a compatible HEAD
		    if (rule.HEAD.subsumes(other_rule.HEAD)) {

			// rule has a more general HEAD than the other. Look if
			// there are some constants in the other pattern that I
			// can use.
			int n_values = 0;
			long[] values = new long[3];
			int[] positions = new int[3];
			for (int i = 0; i < 3; ++i) {
			    if (other_rule.HEAD.p[i].getValue() >= 0
				    && rule.HEAD.p[i].getValue() == Schema.ALL_RESOURCES) {
				values[n_values] = other_rule.HEAD.p[i]
					.getValue();
				positions[n_values] = i;
				n_values++;
			    }
			}

			if (n_values > 0) {
			    // Look at the body of the rule. If we
			    // substitute this value, is the structure going
			    // to be equivalent?

			    List<Pattern> bodyRule = getPatternsInTheBody(rule);

			    // Instantiate the patterns with the values
			    for (int i = 0; i < n_values; ++i) {
				String nameVar = rule.HEAD.p[positions[i]]
					.getName();
				for (Pattern p : bodyRule) {
				    for (int j = 0; j < 3; ++j) {
					if (p.p[j].getName() != null
						&& p.p[j].getName().equals(
							nameVar)) {
					    p.p[j].setValue(values[i]);
					}
				    }
				}
			    }

			    // Remove patterns that are completely instantiated.
			    // If they are not true, the rule couldn't be
			    // applied
			    // anyway because the join will fail.
			    Iterator<Pattern> itr = bodyRule.iterator();
			    while (itr.hasNext()) {
				Pattern p = itr.next();
				boolean completely_instantiated = true;
				for (RDFTerm t : p.p) {
				    if (t.getValue() == Schema.ALL_RESOURCES) {
					completely_instantiated = false;
					break;
				    }
				}
				// FIXME: check also that is precomputed
				if (completely_instantiated) {
				    itr.remove();
				}
			    }

			    // Check whether the two bodies are equivalent
			    List<Pattern> otherBodyRule = getPatternsInTheBody(other_rule);
			    if (equivalent(rule, bodyRule, other_rule,
				    otherBodyRule)) {
				log.info("Rule " + rule.type + " and rule "
					+ other_rule.type
					+ " are equivalent for the values "
					+ Arrays.toString(values));
				for (int i = 0; i < n_values; ++i) {
				    excludedValues[positions[i]].add(values[i]);
				}
			    }
			}
		    }
		}
	    }

	    // Add the excludedValues array to the ruleDef
	    if (excludedValues[0] != null || excludedValues[1] != null
		    || excludedValues[2] != null) {
		rule.head_excludedValues = excludedValues;
	    }
	}
    }

    private List<Pattern> getPatternsInTheBody(Rule1 rule) {
	List<Pattern> list = new ArrayList<Pattern>();

	for (Pattern p : rule.PRECOMPS) {
	    list.add(p.copyOf());
	}

	if (rule instanceof Rule2) {
	    for (Pattern p : ((Rule2) rule).GENERICS_STRATS[0].patterns) {
		list.add(p.copyOf());
	    }
	}

	return list;
    }

    private boolean equivalent(Rule1 rule, List<Pattern> bodyRule,
	    Rule1 other_rule, List<Pattern> otherBodyRule)
	    throws Exception {

	if (bodyRule.size() == otherBodyRule.size()) {

	    // Generate all the possible mappings between the two sides
	    Collection<List<Pattern>> possibleMappings = new ArrayList<List<Pattern>>();
	    for (Pattern p : bodyRule) {
		List<Pattern> matchingPatterns = new ArrayList<Pattern>();
		for (Pattern p1 : otherBodyRule) {
		    if (p.isEquals(p1)) {
			matchingPatterns.add(p1);
		    }
		}

		// There are no matching patterns. The two bodies cannot be
		// equivalent
		if (matchingPatterns.size() == 0) {
		    return false;
		}

		if (possibleMappings.size() == 0) {
		    for (Pattern pattern : matchingPatterns) {
			List<Pattern> newList = new ArrayList<Pattern>();
			newList.add(pattern);
			possibleMappings.add(newList);
		    }
		} else {
		    Collection<List<Pattern>> newMappings = new ArrayList<List<Pattern>>();

		    for (Pattern toAdd : matchingPatterns) {
			for (List<Pattern> existingList : possibleMappings) {
			    boolean isPatternInList = false;
			    for (Pattern patternInList : existingList) {
				if (toAdd == patternInList) {
				    isPatternInList = true;
				}
			    }

			    if (!isPatternInList) {
				List<Pattern> newList = new ArrayList<Pattern>();
				newList.addAll(existingList);
				newList.add(toAdd);
				newMappings.add(newList);
			    }
			}

			if (newMappings.size() == 0) {
			    // No order is found. Return false;
			    return false;
			}
		    }

		    possibleMappings = newMappings;
		}
	    }

	    // Now in possibleMappings there are all possible sequences of p1
	    // that are compatible with p. Verify whether the variables also
	    // corresponds

	    // Make a list of all possible variables in the original pattern
	    List<Long> list1 = createListVariables(bodyRule);
	    for (List<Pattern> possibleMatching : possibleMappings) {
		List<Long> list2 = createListVariables(possibleMatching);
		boolean ok = true;
		for (int i = 0; i < list1.size() && ok; ++i) {
		    if (list1.get(i) != list2.get(i)) {
			ok = false;
		    }
		}

		if (ok)
		    return true;
	    }
	}

	return false;
    }

    private List<Long> createListVariables(List<Pattern> patterns) {
	long count = -2;
	Map<String, Long> map = new HashMap<String, Long>();
	List<Long> output = new ArrayList<Long>();
	for (Pattern p : patterns) {
	    for (RDFTerm t : p.p) {
		if (t.getValue()  == Schema.ALL_RESOURCES) {
		    Long nv = map.get(t.getName());
		    if (nv == null) {
			nv = count--;
			map.put(t.getName(), nv);
		    }
		    output.add(nv);
		} else {
		    output.add(t.getValue());
		}
	    }
	}
	return output;
    }
}