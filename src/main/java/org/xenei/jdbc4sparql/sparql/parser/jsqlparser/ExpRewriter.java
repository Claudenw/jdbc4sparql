package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.GUIDObject;
import org.xenei.jdbc4sparql.iface.name.ItemName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprFunction0;
import org.apache.jena.sparql.expr.ExprFunction1;
import org.apache.jena.sparql.expr.ExprFunction2;
import org.apache.jena.sparql.expr.ExprFunction3;
import org.apache.jena.sparql.expr.ExprFunctionN;
import org.apache.jena.sparql.expr.ExprFunctionOp;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.ExprVisitor;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

public abstract class ExpRewriter implements ExprVisitor {
	protected final Map<ItemName, ItemName> aliasMap = new HashMap<ItemName, ItemName>();
	protected final SparqlQueryBuilder queryBuilder;
	protected final Stack<Expr> stack = new Stack<Expr>();

	public ExpRewriter(final SparqlQueryBuilder queryBuilder) {
		this.queryBuilder = queryBuilder;
	}

	public void addMap(final ItemName from, final ItemName to) {
		aliasMap.put(from, to);
	}

	private ExprList createExprList(final ExprFunction expr) {
		pushArgs(expr);
		final ExprList lst = new ExprList();
		for (int i = 0; i < expr.numArgs(); i++) {
			lst.add(stack.pop());
		}
		return lst;
	}

	public Expr getResult() {
		return stack.pop();
	}

	public ColumnName isMapped(final QueryColumnInfo ci) {
		for (final ItemName qi : aliasMap.keySet()) {
			if (new ColumnName(qi).matches(ci.getName())) {
				final ItemName mapTo = aliasMap.get(qi);
				return new ColumnName(mapTo.getCatalog(), mapTo.getSchema(),
						mapTo.getTable(), ci.getName().getColumn());
			}
		}
		return null;
	}

	/**
	 * Push args in reverse order so we can pop them back in the proper order
	 */
	protected void pushArgs(final ExprFunction exp) {
		for (int i = exp.numArgs(); i > 0; i--) {
			exp.getArg(i).visit(this);
		}
	}

	@Override
	public void visit(final ExprAggregator eAgg) {
		final Aggregator agg = eAgg.getAggregator();
		ExprList exp = agg.getExprList();
		ExprList lst2 = null;
		if (exp != null) {	
			lst2 = new ExprList();
			for (Expr x : exp)
			{
				x.visit(this);
				lst2.add(stack.pop());
			}
		}
		stack.push(new ExprAggregator(eAgg.getVar(), agg.copy(lst2)));
	}

	@Override
	public void visit(final ExprFunction0 func) {
		stack.push(func);
	}

	@Override
	public void visit(final ExprFunction1 func) {
		func.getArg().visit(this);
		stack.push(func.copy(stack.pop()));
	}

	@Override
	public void visit(final ExprFunction2 func) {
		pushArgs(func);
		stack.push(func.copy(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final ExprFunction3 func) {
		pushArgs(func);
		stack.push(func.copy(stack.pop(), stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final ExprFunctionN func) {
		try {
			final Method m = ExprFunctionN.class.getMethod("copy",
					ExprList.class);
			m.setAccessible(true);
			stack.push((Expr) m.invoke(func, createExprList(func)));
		} catch (final NoSuchMethodException e) {
			throw new IllegalStateException(String.format(
					"%s copy(ExprList) method is required", func.getClass()), e);
		} catch (final SecurityException e) {
			throw new IllegalStateException(String.format(
					"Could not make %s copy(ExprList) method visible",
					func.getClass()), e);
		} catch (final IllegalAccessException e) {
			throw new IllegalStateException(String.format(
					"Could not make %s copy(ExprList) method visible",
					func.getClass()), e);
		} catch (final InvocationTargetException e) {
			throw new IllegalStateException(String.format(
					"Could not invoke %s copy(ExprList) method",
					func.getClass()), e);
		}
	}

	@Override
	public void visit(final ExprFunctionOp funcOp) {
		stack.push(funcOp.copy(createExprList(funcOp), funcOp.getGraphPattern()));
	}

	@Override
	public void visit(final ExprVar nv) {
		final Var v = nv.asVar();
		final QueryColumnInfo ci = queryBuilder.getColumn(v);
		if (ci != null) {
			for (final ItemName qi : aliasMap.keySet()) {
				if (new ColumnName(qi).matches(ci.getName())) {
					ItemName mapTo = aliasMap.get(qi);
					mapTo = new ColumnName(mapTo.getCatalog(),
							mapTo.getSchema(), mapTo.getTable(), ci.getName()
							.getColumn());
					stack.push(new ExprVar( GUIDObject.asVar(mapTo)));
					return;
				}
			}
		}
		stack.push(nv);
	}

	@Override
	public void visit(final NodeValue nv) {
		stack.push(nv);
	}

}
