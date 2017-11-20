package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import org.apache.jena.sparql.expr.ExprList;

public class SparqlItemsListVisitor implements ItemsListVisitor {
	private static Logger LOG = LoggerFactory
			.getLogger(SparqlItemsListVisitor.class);
	private final SparqlExprVisitor exprVisitor;
	private ExprList result;

	SparqlItemsListVisitor(final SparqlQueryBuilder builder) {
		exprVisitor = new SparqlExprVisitor(builder,
				SparqlQueryBuilder.OPTIONAL, false);
	}

	public ExprList getResult() {
		return result;
	}

	@Override
	public void visit(final ExpressionList expressionList) {
		if (LOG.isDebugEnabled()) {
			SparqlItemsListVisitor.LOG.debug("visit ExpressionList: {}",
					expressionList);
		}
		@SuppressWarnings("unchecked")
		final List<Expression> l = expressionList.getExpressions();
		result = new ExprList();
		// accept them in reverse order
		for (final Expression e : l) {
			e.accept(exprVisitor);
			result.add(exprVisitor.getResult());
		}
	}

	@Override
	public void visit(final SubSelect subSelect) {
		if (LOG.isDebugEnabled()) {
			SparqlItemsListVisitor.LOG.debug("visit SubSelect: {}", subSelect);
		}
		subSelect.accept(exprVisitor);
	}

}
