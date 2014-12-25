package org.xenei.jdbc4sparql.utils;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;

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
	public void startVisit() {
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
		if (agg.getExpr() != null) {
			agg.getExpr().visit(this);
		}
	}

	@Override
	public void finishVisit() {
	}

}
