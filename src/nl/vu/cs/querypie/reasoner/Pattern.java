package nl.vu.cs.querypie.reasoner;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;

public class Pattern {

	public RDFTerm[] p = { new RDFTerm(), new RDFTerm(), new RDFTerm() };

	private boolean filter = false;

	public boolean isFilter() {
		return filter;
	}

	public void setFilter(boolean filter) {
		this.filter = filter;
	}

	private boolean isEquivalent = false;
	private String location = null;

	public Pattern copyOf() {
		Pattern p = new Pattern();
		for (int i = 0; i < 3; ++i) {
			RDFTerm term = new RDFTerm(this.p[i].getValue());
			term.setName(this.p[i].getName());
			p.p[i] = term;
		}
		return p;
	}

	public List<String> getAllVars() {
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < 3; ++i) {
			if (p[i].getName() != null) {
				list.add(p[i].getName());
			}
		}
		return list;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public void setEquivalent(boolean isEquivalent) {
		this.isEquivalent = isEquivalent;
	}

	public boolean isEquivalent() {
		return isEquivalent;
	}

	public String getSignature() {
		String name = "";
		for (RDFTerm t : p) {
			if (t.getName() != null) {
				name += "* ";
			} else {
				name += t.getValue() + " ";
			}
		}
		return name.substring(0, name.length() - 1);
	}

	@Override
	public String toString() {
		String name = "";
		for (RDFTerm t : p) {
			if (t.getName() != null) {
				name += t.getName() + " ";
			} else {
				name += t.getValue() + " ";
			}
		}
		return name.substring(0, name.length() - 1);
	}

	public int[] getPositionVars() {
		List<Integer> list = new ArrayList<Integer>();

		int j = 0;
		for (RDFTerm t : p) {
			if (t.getName() != null) {
				list.add(j);
			}
			j++;
		}

		int[] v = new int[list.size()];
		for (int m = 0; m < list.size(); ++m) {
			v[m] = list.get(m);
		}
		return v;
	}

	public List<String> calculateSharedVars(Pattern pattern) {
		List<String> output = new ArrayList<String>();
		for (RDFTerm term : p) {
			String name = term.getName();
			if (name != null) {
				for (RDFTerm t_p : pattern.p) {
					String name2 = t_p.getName();
					if (name2 != null && name.equals(name2)) {
						output.add(name);
					}
				}
			}
		}
		return output;
	}

	public List<String[]> calculateEquivalenceMapping(Pattern p) {
		List<String[]> associations = new ArrayList<String[]>();

		for (int i = 0; i < 3; ++i) {
			RDFTerm t1 = this.p[i];
			RDFTerm t2 = p.p[i];

			if (t1.getName() != null && t2.getName() != null) {

				// Check whether it already appeared
				for (String[] existingAssociations : associations) {
					if ((existingAssociations[0].equals(t1.getName()) && !existingAssociations[1]
							.equals(t2.getName()))
							|| (!existingAssociations[0].equals(t1.getName()) && existingAssociations[1]
									.equals(t2.getName()))) {
						return null;
					}
				}

				String[] ass = new String[2];
				ass[0] = t1.getName();
				ass[1] = t2.getName();
				associations.add(ass);
			} else if (t1.getName() != null
					|| (t2.getName() != null || t2.getValue() != t1.getValue()))
				return null;
		}

		return associations;
	}

	public boolean subsumes(Pattern pattern) {
		for (int i = 0; i < p.length; ++i) {
			if (p[i].getValue() != pattern.p[i].getValue()
					&& p[i].getValue() != Schema.ALL_RESOURCES) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		return getSignature().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return getSignature().equals(((Pattern) obj).getSignature());
	}

	public boolean isEquals(Pattern p1) {
		for (int i = 0; i < 3; i++) {
			if (p[i].getValue() != p1.p[i].getValue()) {
				return false;
			}
		}
		return true;
	}

	public int getPosVar(String string) {
		for (int i = 0; i < p.length; ++i) {
			if (p[i].getName() != null && p[i].getName().equals(string)) {
				return i;
			}
		}
		return -1;
	}
}