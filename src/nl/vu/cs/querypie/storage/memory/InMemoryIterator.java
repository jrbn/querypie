package nl.vu.cs.querypie.storage.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.querypie.storage.RDFTerm;
import nl.vu.cs.querypie.storage.Schema;
import nl.vu.cs.querypie.storage.TripleIterator;
import nl.vu.cs.querypie.storage.disk.EmptyIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InMemoryIterator extends TupleIterator implements
		TripleIterator {

	static final Logger log = LoggerFactory.getLogger(InMemoryIterator.class);
	public static final Collection<Triple> EMPTYTRIPLES = new ArrayList<Triple>();
	public static final Iterator<Triple> EMPTYITER = EMPTYTRIPLES.iterator();

	abstract long doEstimate() throws Exception;

	@Override
	public long estimateRecords() throws Exception {
		long count = doEstimate();
		if (count > Integer.MAX_VALUE) {
			// Limit value to prevent overflow in case of addition.
			count = Integer.MAX_VALUE;
		}
		return count;
	}

	@Override
	public void stopReading() {
	}

	private static class InMemoryIteratorFixedObjectPredicate extends
			InMemoryIterator {

		Iterator<Triple> tripleIter = null;
		Collection<Triple> triples = null;
		Collection<Long> subjects;

		InMemoryIteratorFixedObjectPredicate(Collection<Long> subjects,
				long filter_p, Collection<Triple> triples) {
			super();
			this.subjects = subjects;
			possibleKeys = subjects.iterator();
			if (triples != null) {
				this.triples = triples;
				tripleIter = triples.iterator();
			} else {
				this.triples = EMPTYTRIPLES;
				tripleIter = EMPTYITER;
			}
			this.filter_p = filter_p;
		}

		@Override
		public boolean next() throws Exception {
			if (!tripleIter.hasNext() || !possibleKeys.hasNext()) {
				return false;
			}
			currentTriple = tripleIter.next();
			long key = possibleKeys.next();

			for (;;) {
				while (currentTriple.subject < key) {
					if (!tripleIter.hasNext()) {
						return false;
					}
					currentTriple = tripleIter.next();
				}
				while (currentTriple.subject == key) {
					if (currentTriple.predicate < filter_p) {
						if (!tripleIter.hasNext()) {
							return false;
						}
						currentTriple = tripleIter.next();
						continue;
					}
					if (currentTriple.predicate == filter_p) {
						return true;
					}
					// No more possibilities for this key.
					if (!possibleKeys.hasNext() || !tripleIter.hasNext()) {
						return false;
					}
					key = possibleKeys.next();
					currentTriple = tripleIter.next();
					break;
				}

				while (currentTriple.subject > key) {
					if (!possibleKeys.hasNext()) {
						return false;
					}
					key = possibleKeys.next();
				}
			}
		}

		@Override
		public long doEstimate() throws Exception {
			// This may be too expensive, but gives an accurate estimate.
			ArrayList<Triple> l = new ArrayList<Triple>();
			for (Triple t : triples) {
				if (t.predicate == filter_p) {
					l.add(t);
				}
			}

			long count = 0;
			int tripleIndex = 0;
			for (Long s : subjects) {
				for (;;) {
					if (tripleIndex >= l.size()) {
						return count;
					}
					Triple t = l.get(tripleIndex);
					if (t.subject < s) {
						tripleIndex++;
						continue;
					}
					if (t.subject > s) {
						break;
					}
					count++;
					tripleIndex++;
				}
			}

			return count;
		}
	}

	private static class InMemoryIteratorSubjectVar extends InMemoryIterator {

		Iterator<Triple> tripleIter = null;
		long key;
		Collection<Triple> triples = null;
		Collection<Long> subjects;

		InMemoryIteratorSubjectVar(Collection<Long> subjects,
				Collection<Triple> triples) {
			super();
			possibleKeys = subjects.iterator();
			if (triples == null || !possibleKeys.hasNext()) {
				tripleIter = EMPTYITER;
				this.triples = EMPTYTRIPLES;
			} else {
				tripleIter = triples.iterator();
				this.triples = triples;
				key = possibleKeys.next();
			}
			this.subjects = subjects;
		}

		@Override
		public boolean next() throws Exception {

			if (!tripleIter.hasNext()) {
				return false;
			}

			currentTriple = tripleIter.next();

			for (;;) {
				while (currentTriple.subject < key) {
					if (!tripleIter.hasNext()) {
						return false;
					}
					currentTriple = tripleIter.next();
				}

				while (currentTriple.subject > key) {
					if (!possibleKeys.hasNext()) {
						return false;
					}
					key = possibleKeys.next();
				}
				if (currentTriple.subject == key) {
					return true;
				}
			}
		}

		@Override
		public long doEstimate() throws Exception {
			// This may be too expensive, but gives an accurate estimate.
			ArrayList<Triple> l = new ArrayList<Triple>(triples);
			long count = 0;
			int tripleIndex = 0;
			for (Long s : subjects) {
				for (;;) {
					if (tripleIndex >= l.size()) {
						return count;
					}
					Triple t = l.get(tripleIndex);
					if (t.subject < s) {
						tripleIndex++;
						continue;
					}
					if (t.subject > s) {
						break;
					}
					count++;
					tripleIndex++;
				}
			}

			return count;
		}
	}

	private static class InMemoryIteratorVar1NoFilterP extends InMemoryIterator {

		InMemoryIteratorVar1NoFilterP(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_o) throws Exception {
			super(map, col, -2, -1, filter_o, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.object == filter_o) {
					return true;
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.object == filter_o) {
						count++;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar1NoFilterO extends InMemoryIterator {

		InMemoryIteratorVar1NoFilterO(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_p) throws Exception {
			super(map, col, -2, filter_p, -1, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.predicate == filter_p) {
					return true;
				}
				if (currentTriple.predicate > filter_p) {
					itr = advance_next_key();
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.predicate == filter_p) {
						count++;
					}
					if (t.predicate > filter_p) {
						break;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar1NoFilterPO extends
			InMemoryIterator {

		InMemoryIteratorVar1NoFilterPO(Map<Long, Collection<Triple>> map,
				Collection<Long> col) throws Exception {
			super(map, col, -2, -1, -1, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				sum += map.get(l).size();
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar1 extends InMemoryIterator {

		InMemoryIteratorVar1(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_p, long filter_o)
				throws Exception {
			super(map, col, -2, filter_p, filter_o, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.predicate != filter_p) {
					if (currentTriple.predicate > filter_p) {
						itr = advance_next_key();
					}
					continue;
				}
				if (currentTriple.object == filter_o) {
					return true;
				}
				if (currentTriple.object > filter_o) {
					itr = advance_next_key();
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.predicate < filter_p) {
						continue;
					}
					if (t.predicate > filter_p) {
						break;
					}
					if (t.object == filter_o) {
						count++;
					}
					if (t.object > filter_o) {
						break;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar2 extends InMemoryIterator {

		InMemoryIteratorVar2(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_s, long filter_o)
				throws Exception {
			super(map, col, filter_s, -2, filter_o, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (filter_s >= 0 && currentTriple.subject != filter_s) {
					if (currentTriple.subject > filter_s) {
						itr = advance_next_key();
					}
					continue;
				}
				if (filter_o >= 0 && currentTriple.object != filter_o) {
					if (currentTriple.object > filter_o) {
						if (filter_s != -1) {
							itr = advance_next_key();
						}
					}
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (filter_s >= 0 && t.subject != filter_s) {
						if (t.subject > filter_s) {
							break;
						}
						continue;
					}
					if (filter_o >= 0 && t.object != filter_o) {
						if (t.object > filter_o) {
							if (filter_s != -1) {
								break;
							}
						}
						continue;
					}
					count++;
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar2NoFilterS extends InMemoryIterator {

		InMemoryIteratorVar2NoFilterS(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_o) throws Exception {
			super(map, col, -1, -2, filter_o, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.object == filter_o) {
					return true;
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.object == filter_o) {
						count++;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar2NoFilterO extends InMemoryIterator {

		InMemoryIteratorVar2NoFilterO(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_s) throws Exception {
			super(map, col, filter_s, -2, -1, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.subject == filter_s) {
					return true;
				}
				if (currentTriple.subject > filter_s) {
					itr = advance_next_key();
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.subject > filter_s) {
						break;
					}
					if (t.subject == filter_s) {
						count++;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar2NoFilterSO extends
			InMemoryIterator {

		InMemoryIteratorVar2NoFilterSO(Map<Long, Collection<Triple>> map,
				Collection<Long> col) throws Exception {
			super(map, col, -1, -2, -1, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				sum += map.get(l).size();
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar3 extends InMemoryIterator {

		InMemoryIteratorVar3(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_s, long filter_p)
				throws Exception {
			super(map, col, filter_s, filter_p, -2, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.subject != filter_s) {
					if (currentTriple.subject > filter_s) {
						itr = advance_next_key();
					}
					continue;
				}
				if (currentTriple.predicate != filter_p) {
					if (currentTriple.predicate > filter_p) {
						itr = advance_next_key();
					}
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.subject != filter_s) {
						if (t.subject > filter_s) {
							break;
						}
						continue;
					}
					if (t.predicate != filter_p) {
						if (t.predicate > filter_p) {
							break;
						}
						continue;
					}
					count++;
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar3NoFilterS extends InMemoryIterator {

		InMemoryIteratorVar3NoFilterS(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_p) throws Exception {
			super(map, col, -1, filter_p, -2, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.predicate == filter_p) {
					return true;
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.predicate == filter_p) {
						count++;
					}
				}
			}
			return count;
		}

	}

	private static class InMemoryIteratorVar3NoFilterP extends InMemoryIterator {

		InMemoryIteratorVar3NoFilterP(Map<Long, Collection<Triple>> map,
				Collection<Long> col, long filter_s) throws Exception {
			super(map, col, filter_s, -1, -2, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (currentTriple.subject == filter_s) {
					return true;
				}
				if (currentTriple.subject > filter_s) {
					itr = advance_next_key();
				}
			}
			return false;
		}

		@Override
		public long doEstimate() throws Exception {
			long count = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (t.subject > filter_s) {
						break;
					}
					if (t.subject == filter_s) {
						count++;
					}
				}
			}
			return count;
		}
	}

	private static class InMemoryIteratorVar3NoFilterSP extends
			InMemoryIterator {

		InMemoryIteratorVar3NoFilterSP(Map<Long, Collection<Triple>> map,
				Collection<Long> col) throws Exception {
			super(map, col, -1, -1, -2, null, null, null);
		}

		@Override
		public boolean next() {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				sum += map.get(l).size();
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar3FilterSets extends
			InMemoryIterator {

		InMemoryIteratorVar3FilterSets(Map<Long, Collection<Triple>> map,
				Collection<Long> col, Collection<Long> set1,
				Collection<Long> set2) throws Exception {
			super(map, col, -3, -3, -2, set1, set2, null);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_p.contains(currentTriple.predicate)
						|| !possible_s.contains(currentTriple.subject)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_p.contains(t.predicate)
							&& possible_s.contains(t.subject)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar1FilterSets extends
			InMemoryIterator {

		InMemoryIteratorVar1FilterSets(Map<Long, Collection<Triple>> map,
				Collection<Long> col, Collection<Long> set2,
				Collection<Long> set3) throws Exception {
			super(map, col, -2, -3, -3, null, set2, set3);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_p.contains(currentTriple.predicate)
						|| !possible_o.contains(currentTriple.object)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_p.contains(t.predicate)
							&& possible_o.contains(t.object)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar1FilterV2SetV3Unbound extends
			InMemoryIterator {

		InMemoryIteratorVar1FilterV2SetV3Unbound(
				Map<Long, Collection<Triple>> map, Collection<Long> col,
				Collection<Long> set) throws Exception {
			super(map, col, -2, -3, -1, null, set, null);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_p.contains(currentTriple.predicate)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_p.contains(t.predicate)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar1FilterV2UnboundV3Set extends
			InMemoryIterator {

		InMemoryIteratorVar1FilterV2UnboundV3Set(
				Map<Long, Collection<Triple>> map, Collection<Long> col,
				Collection<Long> set) throws Exception {
			super(map, col, -2, -1, -3, null, null, set);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_o.contains(currentTriple.object)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_o.contains(t.object)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar2FilterSets extends
			InMemoryIterator {

		InMemoryIteratorVar2FilterSets(Map<Long, Collection<Triple>> map,
				Collection<Long> col, Collection<Long> set1,
				Collection<Long> set3) throws Exception {
			super(map, col, -3, -2, -3, set1, null, set3);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_s.contains(currentTriple.subject)
						|| !possible_o.contains(currentTriple.object)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_s.contains(t.subject)
							&& possible_o.contains(t.object)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	private static class InMemoryIteratorVar2FilterV1UnboundV3Set extends
			InMemoryIterator {

		InMemoryIteratorVar2FilterV1UnboundV3Set(
				Map<Long, Collection<Triple>> map, Collection<Long> col,
				Collection<Long> set) throws Exception {
			super(map, col, -1, -2, -3, null, null, set);
		}

		@Override
		public boolean next() throws Exception {
			while (itr != null) {
				if (!itr.hasNext()) {
					itr = advance_next_key();
					continue;
				}
				currentTriple = itr.next();
				if (!possible_o.contains(currentTriple.object)) {
					continue;
				}
				return true;
			}
			return false;
		}

		@Override
		public long doEstimate() {
			long sum = 0;
			for (Long l : col) {
				Collection<Triple> c = map.get(l);
				for (Triple t : c) {
					if (possible_o.contains(t.object)) {
						sum++;
					}
				}
			}
			return sum;
		}
	}

	Iterator<Triple> itr = null;
	RDFTerm s = new RDFTerm(), o = new RDFTerm(), p = new RDFTerm();
	Triple currentTriple;
	Map<Long, Collection<Triple>> map;
	Iterator<Long> possibleKeys;

	long filter_s, filter_p, filter_o;
	Collection<Long> possible_s, possible_p, possible_o;
	Collection<Long> col;

	private InMemoryIterator(Map<Long, Collection<Triple>> map,
			Collection<Long> col, long filter_s, long filter_p, long filter_o,
			Collection<Long> possible_s, Collection<Long> possible_p,
			Collection<Long> possible_o) throws Exception {
		this.filter_s = filter_s;
		this.filter_p = filter_p;
		this.filter_o = filter_o;
		this.possible_s = possible_s;
		this.possible_p = possible_p;
		this.possible_o = possible_o;
		this.col = col;

		this.map = map;
		if (col != null) {
			possibleKeys = col.iterator();
			itr = advance_next_key();
		} else {
			throw new Exception("Something strange. Could not find the set.");
		}
	}

	public InMemoryIterator() {
	}

	@SuppressWarnings("unchecked")
	public static TupleIterator getIterator(ActionContext context,
			InMemoryTripleContainer c, long v1, long v2, long v3)
			throws Exception {

		Schema schema = Schema.getInstance();

		if (log.isDebugEnabled()) {
			log.debug("getIterator: v1 = " + v1 + ", v2 = " + v2 + ", v3 = "
					+ v3);
		}

		if (v1 == Schema.SCHEMA_SUBSET) {
			return new EmptyIterator();
		}

		Collection<Long> values = null;
		Map<Long, Collection<Triple>> map;

		if (v1 <= Schema.SET_THRESHOLD) {
			values = schema.getSubset(v1, context);
			if (values == null || values.size() == 0) {
				return new EmptyIterator();
			}
			if (log.isDebugEnabled()) {
				log.debug("getIterator: v1 = " + v1 + ", values size = "
						+ values.size());
			}

			// Addition to handle two sets
			if (v2 <= Schema.SET_THRESHOLD) { // Three sets are not supported
				if (v3 >= 0) {
					values = new ArrayList<Long>();
					values.add(v3);
					return new InMemoryIteratorVar3FilterSets(
							c.getTriplesIndexedByObject(), values,
							schema.getSubset(v1, context), schema.getSubset(v2,
									context));
				} else if (v3 == Schema.ALL_RESOURCES) {
					return new InMemoryIteratorVar1FilterV2SetV3Unbound(
							c.getTriplesIndexedBySubject(), values,
							schema.getSubset(v2, context));
				} else {
					throw new Exception("Three sets are not supported");
				}
			}
			if (v3 <= Schema.SET_THRESHOLD) {
				if (v2 == Schema.ALL_RESOURCES) {
					return new InMemoryIteratorVar1FilterV2UnboundV3Set(
							c.getTriplesIndexedBySubject(), values,
							schema.getSubset(v3, context));
				} else {
					values = new ArrayList<Long>();
					values.add(v2);
					return new InMemoryIteratorVar2FilterSets(
							c.getTriplesIndexedByPredicate(), values,
							schema.getSubset(v1, context), schema.getSubset(v3,
									context));
				}
			}

			if (values.size() > 10) {
				if (v3 >= 0) {
					map = c.getTriplesIndexedByObject();
					Collection<Triple> set = map.get(v3);
					if (v2 >= 0) {
						return new InMemoryIteratorFixedObjectPredicate(values,
								v2, set);
					}
					return new InMemoryIteratorSubjectVar(values, set);
				}
				if (v2 >= 0) {
					return new InMemoryIteratorSubjectVar(values, c
							.getTriplesIndexedByPredicate().get(v2));
				}
			}
			map = c.getTriplesIndexedBySubject();
			if (v2 >= 0) {
				if (v3 >= 0) {
					return new InMemoryIteratorVar1(map, values, v2, v3);
				}
				return new InMemoryIteratorVar1NoFilterO(map, values, v2);
			}
			if (v3 >= 0) {
				return new InMemoryIteratorVar1NoFilterP(map, values, v3);
			}
			return new InMemoryIteratorVar1NoFilterPO(map, values);
		}

		if (v2 <= Schema.SET_THRESHOLD) {
			if (v2 <= RDFTerm.THRESHOLD_VARIABLE) {
				values = (Collection<Long>) context.getObjectFromCache(v2);
			} else {
				values = Schema.getInstance().getSubset(v2);
				if (values == null || values.size() == 0) {
					return new EmptyIterator();
				}
			}

			if (v3 <= Schema.SET_THRESHOLD) {
				if (v1 >= 0) {
					values = new ArrayList<Long>();
					values.add(v2);
					return new InMemoryIteratorVar1FilterSets(
							c.getTriplesIndexedBySubject(), values,
							schema.getSubset(v2, context), schema.getSubset(v3,
									context));
				} else {
					return new InMemoryIteratorVar2FilterV1UnboundV3Set(
							c.getTriplesIndexedByPredicate(), values,
							schema.getSubset(v3, context));
				}
			}

			map = c.getTriplesIndexedByPredicate();
			if (v1 >= 0) {
				if (v3 >= 0) {
					return new InMemoryIteratorVar2(map, values, v1, v3);
				}
				return new InMemoryIteratorVar2NoFilterO(map, values, v1);
			}
			if (v3 >= 0) {
				return new InMemoryIteratorVar2NoFilterS(map, values, v3);
			}
			return new InMemoryIteratorVar2NoFilterSO(map, values);
		}

		if (v3 <= RDFTerm.THRESHOLD_VARIABLE) {
			values = (Collection<Long>) context.getObjectFromCache(v3);
		} else if (v3 <= Schema.SET_THRESHOLD) {
			values = Schema.getInstance().getSubset(v3);
			if (values == null || values.size() == 0) {
				return new EmptyIterator();
			}
		} else if (v3 >= 0) {
			values = new ArrayList<Long>();
			values.add(v3);
		}
		if (v3 != -1) {
			map = c.getTriplesIndexedByObject();
			if (v1 >= 0) {
				if (v2 >= 0) {
					return new InMemoryIteratorVar3(map, values, v1, v2);
				}
				return new InMemoryIteratorVar3NoFilterP(map, values, v1);
			}
			if (v2 >= 0) {
				return new InMemoryIteratorVar3NoFilterS(map, values, v2);
			}
			return new InMemoryIteratorVar3NoFilterSP(map, values);
		}

		// Now v3 = -1.
		if (v2 >= 0) {
			map = c.getTriplesIndexedByPredicate();
			values = new ArrayList<Long>();
			values.add(v2);
			if (v1 >= 0) {
				return new InMemoryIteratorVar2NoFilterO(map, values, v1);
			}
			return new InMemoryIteratorVar2NoFilterSO(map, values);
		}
		// Now v2 = -1 as well.
		map = c.getTriplesIndexedBySubject();
		if (v1 < 0) {
			// Is this possible? Can we have -1 -1 -1?
			values = map.keySet();
		} else {
			values = new ArrayList<Long>();
			values.add(v1);
		}
		return new InMemoryIteratorVar1NoFilterPO(map, values);
	}

	Iterator<Triple> advance_next_key() {
		while (possibleKeys.hasNext()) {
			long key = possibleKeys.next();
			Collection<Triple> current_collection = map.get(key);
			if (current_collection != null) {
				if (log.isDebugEnabled()) {
					log.debug("Advance_next_key returns set of size "
							+ current_collection.size());
				}
				return current_collection.iterator();
			}
		}
		return null;
	}

	@Override
	public void getTuple(Tuple tuple) throws Exception {
		s.setValue(currentTriple.subject);
		p.setValue(currentTriple.predicate);
		o.setValue(currentTriple.object);
		tuple.set(s, p, o);
	}

	@Override
	public boolean isReady() {
		return true;
	}
}
