package nl.vu.cs.querypie.sparql;

import java.util.List;

import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class QueryVisitor extends QueryModelVisitorBase<RuntimeException> {

	List<StatementPattern> list = null;

	public QueryVisitor(List<StatementPattern> list) {
		this.list = list;
	}

	@Override
	public void meet(Join op) { // TODO: Good query evaluation
		op.getLeftArg().visit(this);
		op.getRightArg().visit(this);
	}

	@Override
	public void meet(StatementPattern sp) {
		list.add(sp);
	}
}
