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
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementAssign;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementData;
import com.hp.hpl.jena.sparql.syntax.ElementDataset;
import com.hp.hpl.jena.sparql.syntax.ElementExists;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementNotExists;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;

/**
 * Class for test classes to extract element types from query.
 */
public class ExpressionExtractor implements ExprVisitor {
	private List<Expr> extracted = new ArrayList<Expr>();
	private Class<? extends Expr> matchType;

	/**
	 * Set the type to match
	 * 
	 * @param clazz
	 *            The class type to match
	 * @return this ElementExtractor for chaining
	 */
	public ExpressionExtractor setMatchType(Class<? extends Expr> clazz) {
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

	public ExpressionExtractor(Class<? extends Expr> clazz) {
		setMatchType(clazz);
	}

	@Override
	public void startVisit() {
	}

	@Override
	public void visit(ExprFunction0 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (Expr e : func.getArgs())
		{
			e.visit( this );
		}
	}

	@Override
	public void visit(ExprFunction1 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (Expr e : func.getArgs())
		{
			e.visit( this );
		}
	}

	@Override
	public void visit(ExprFunction2 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (Expr e : func.getArgs())
		{
			e.visit( this );
		}

	}

	@Override
	public void visit(ExprFunction3 func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (Expr e : func.getArgs())
		{
			e.visit( this );
		}
	}

	@Override
	public void visit(ExprFunctionN func) {
		if (matchType.isAssignableFrom(func.getClass())) {
			extracted.add(func);
		}
		for (Expr e : func.getArgs())
		{
			e.visit( this );
		}
	}

	@Override
	public void visit(ExprFunctionOp funcOp) {
		if (matchType.isAssignableFrom(funcOp.getClass())) {
			extracted.add(funcOp);
		}
		for (Expr e : funcOp.getArgs())
		{
			e.visit( this );
		}
	}

	@Override
	public void visit(NodeValue nv) {
		if (matchType.isAssignableFrom(nv.getClass())) {
			extracted.add(nv);
		}
	}

	@Override
	public void visit(ExprVar nv) {
		if (matchType.isAssignableFrom(nv.getClass())) {
			extracted.add(nv);
		}
	}

	@Override
	public void visit(ExprAggregator eAgg) {
		if (matchType.isAssignableFrom(eAgg.getClass())) {
			extracted.add(eAgg);
		}
		eAgg.getAggVar().visit( this );
		Aggregator agg = eAgg.getAggregator();
		if (agg.getExpr() != null)
		{
			agg.getExpr().visit(this);
		}
	}

	@Override
	public void finishVisit() {		
	}

}
