package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.xenei.jdbc4sparql.sparql.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.QueryItemName;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public abstract class ExpRewriter implements ExprVisitor
{
	protected final Map<QueryItemName, QueryItemName> aliasMap = new HashMap<QueryItemName, 
			QueryItemName>();
	protected final SparqlQueryBuilder queryBuilder;
	protected final Stack<Expr> stack = new Stack<Expr>();

	public ExpRewriter( final SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}

	

	public void addMap( final QueryItemName from, final QueryItemName to )
	{
		aliasMap.put(from, to);
	}

	private ExprList createExprList( final ExprFunction expr )
	{
		pushArgs(expr);
		final ExprList lst = new ExprList();
		for (int i = 0; i < expr.numArgs(); i++)
		{
			lst.add(stack.pop());
		}
		return lst;
	}

	@Override
	public void finishVisit()
	{
	}

	public Expr getResult()
	{
		return stack.pop();
	}

	public QueryColumnInfo.Name isMapped( final QueryColumnInfo ci )
	{
		for (final QueryItemName qi : aliasMap.keySet())
		{
			if (QueryColumnInfo.getNameInstance(qi).matches(ci.getName()))
			{
				final QueryItemName mapTo = aliasMap.get(qi);
				return QueryColumnInfo.getNameInstance(mapTo.getSchema(),
						mapTo.getTable(), ci.getName().getCol());
			}
		}
		return null;
	}

	/**
	 * Push args in reverse order so we can pop them back in the proper order
	 */
	protected void pushArgs( final ExprFunction exp )
	{
		for (int i = exp.numArgs(); i > 0; i--)
		{
			exp.getArg(i).visit(this);
		}
	}

	@Override
	public void startVisit()
	{
	}

	@Override
	public void visit( final ExprAggregator eAgg )
	{
		final Aggregator agg = eAgg.getAggregator();
		Expr exp = agg.getExpr();
		if (exp != null)
		{
			exp.visit(this);
			exp = stack.pop();
		}
		stack.push(new ExprAggregator(eAgg.getVar(), agg.copy(exp)));
	}

	@Override
	public void visit( final ExprFunction0 func )
	{
		stack.push(func);
	}

	@Override
	public void visit( final ExprFunction1 func )
	{
		func.getArg().visit(this);
		stack.push(func.copy(stack.pop()));
	}

	@Override
	public void visit( final ExprFunction2 func )
	{
		pushArgs(func);
		stack.push(func.copy(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final ExprFunction3 func )
	{
		pushArgs(func);
		stack.push(func.copy(stack.pop(), stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final ExprFunctionN func )
	{
		try
		{
			final Method m = ExprFunctionN.class.getMethod("copy",
					ExprList.class);
			m.setAccessible(true);
			stack.push((Expr) m.invoke(func, createExprList(func)));
		}
		catch (final NoSuchMethodException e)
		{
			throw new IllegalStateException(String.format(
					"%s copy(ExprList) method is required", func.getClass()), e);
		}
		catch (final SecurityException e)
		{
			throw new IllegalStateException(String.format(
					"Could not make %s copy(ExprList) method visible",
					func.getClass()), e);
		}
		catch (final IllegalAccessException e)
		{
			throw new IllegalStateException(String.format(
					"Could not make %s copy(ExprList) method visible",
					func.getClass()), e);
		}
		catch (final InvocationTargetException e)
		{
			throw new IllegalStateException(String.format(
					"Could not invoke %s copy(ExprList) method",
					func.getClass()), e);
		}
	}

	@Override
	public void visit( final ExprFunctionOp funcOp )
	{
		stack.push(funcOp.copy(createExprList(funcOp), funcOp.getGraphPattern()));
	}

	@Override
	public void visit( final ExprVar nv )
	{
		final Node n = nv.getAsNode();
		final QueryColumnInfo ci = queryBuilder.getNodeColumn(n);
		if (ci != null)
		{
			for (final QueryItemName qi : aliasMap.keySet())
			{
				if (QueryColumnInfo.getNameInstance(qi).matches(ci.getName()))
				{
					QueryItemName mapTo = aliasMap.get(qi);
					mapTo = QueryColumnInfo.getNameInstance(mapTo.getSchema(),
							mapTo.getTable(), ci.getName().getCol());
					stack.push(new ExprVar(mapTo.getSPARQLName()));
					return;
				}
			}
		}
		stack.push(nv);
	}

	@Override
	public void visit( final NodeValue nv )
	{
		stack.push(nv);
	}

}
