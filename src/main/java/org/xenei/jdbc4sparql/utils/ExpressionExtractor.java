package org.xenei.jdbc4sparql.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprNone;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprVisitor;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

/**
 * Class for test classes to extract element types from query.
 */
public class ExpressionExtractor implements ExprVisitor {
	private final List<Expr> extracted = new ArrayList<Expr>();
	private Class<? extends Expr> matchType;

	/**
	 * Set the type to match
	 *
	 * @param clazz
	 *            The class type to match
	 * @return this ElementExtractor for chaining
	 */
	public ExpressionExtractor setMatchType(final Class<? extends Expr> clazz) {
		matchType = clazz;
		return this;
	}

	/**
	 * Reset the results.
	 *
	 * @return this ElementExtractor for chaining
	 */
	public ExpressionExtractor reset() {
		extracted.clear();
		return this;
	}

	public List<Expr> getExtracted() {
		return extracted;
	}

	public ExpressionExtractor(final Class<? extends Expr> clazz) {
		setMatchType(clazz);
	}

	@Override
	public void visit(final ExprFunction0 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (final Expr e : func.getArgs()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ExprFunction1 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (final Expr e : func.getArgs()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ExprFunction2 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (final Expr e : func.getArgs()) {
			e.visit(this);
		}

	}

	@Override
	public void visit(final ExprFunction3 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (final Expr e : func.getArgs()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ExprFunctionN func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (final Expr e : func.getArgs()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final ExprFunctionOp funcOp) {
		if (matchType.isAssignableFrom(funcOp.getClass())) {
			extracted.add(funcOp);
		}
		for (final Expr e : funcOp.getArgs()) {
			e.visit(this);
		}
	}

	@Override
	public void visit(final NodeValue nv) {
		if (matchType.isAssignableFrom(nv.getClass())) {
			extracted.add(nv);
		}
	}

	@Override
	public void visit(final ExprVar nv) {
		if (matchType.isAssignableFrom(nv.getClass())) {
			extracted.add(nv);
		}
	}

	@Override
	public void visit(final ExprAggregator eAgg) {
		if (matchType.isAssignableFrom(eAgg.getClass())) {
			extracted.add(eAgg);
		}
		eAgg.getAggVar().visit(this);
		final Aggregator agg = eAgg.getAggregator();
		if (agg.getExprList() != null) {
			for (Expr x : agg.getExprList() ) {
				x.visit(this);
			}
		}
	}

	@Override
	public void visit(ExprNone exprNone) {
		if (matchType.isAssignableFrom(exprNone.getClass())) {
			extracted.add(exprNone);
		}
	}

}
