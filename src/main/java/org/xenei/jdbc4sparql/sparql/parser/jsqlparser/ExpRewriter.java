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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.xenei.jdbc4sparql.sparql.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.QueryItemName;
import org.xenei.jdbc4sparql.sparql.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

public class ExpRewriter implements ExprVisitor
{
	private Map<QueryItemName, QueryItemName> aliasMap = new HashMap<QueryItemName, QueryItemName>();
	private SparqlQueryBuilder queryBuilder;
	private Stack<Expr> stack = new Stack<Expr>();
	
	public ExpRewriter(SparqlQueryBuilder queryBuilder )
	{
		this.queryBuilder = queryBuilder;
	}
	
	public Expr getResult()
	{
		return stack.pop();
	}
	
	public void addMap(QueryItemName from, QueryItemName to)
	{
		aliasMap.put( from, to );
	}
	
	@Override
	public void startVisit()
	{
	}

	@Override
	public void visit( ExprFunction0 func )
	{
		stack.push( func );
	}

	@Override
	public void visit( ExprFunction1 func )
	{
		func.getArg().visit(this);
		stack.push( func.copy( stack.pop() ));
	}
	
	/**
	 * Push args in reverse order so we can pop them back in the proper order
	 */
	private void pushArgs( ExprFunction exp )
	{
		for (int i=exp.numArgs();i>0;i--)
		{
			exp.getArg(i).visit( this );
		}
	}
	
	public QueryItemName isMapped(QueryColumnInfo ci)
	{
		for (QueryItemName qi : aliasMap.keySet())
		{
			if (QueryColumnInfo.getNameInstance(qi).matches( ci.getName()))
			{
				QueryItemName mapTo = aliasMap.get(qi);
				return QueryColumnInfo.getNameInstance(mapTo.getSchema(), mapTo.getTable(), ci.getName().getCol());
			}
		}
		return null;
	}
	
	private Node addAlias( QueryColumnInfo columnInfo, QueryItemName alias)
	{
		QueryTableInfo.Name qtn = QueryTableInfo.getNameInstance( columnInfo.getName() );
		QueryTableInfo tableInfo = queryBuilder.getTable( qtn );
		return tableInfo.addColumn(new QueryColumnInfo( columnInfo, alias ));
	}

	@Override
	public void visit( ExprFunction2 func )
	{
		if (func instanceof E_Equals)
		{
			E_Equals eq = (E_Equals)func;
			if ((eq.getArg1() instanceof ExprVar) &&
					(eq.getArg2() instanceof ExprVar))
			{
				final QueryColumnInfo ci1 = queryBuilder.getNodeColumn(((ExprVar) eq.getArg1()).getAsNode());
				if (ci1 != null)
				{
					final QueryColumnInfo ci2 = queryBuilder.getNodeColumn(((ExprVar) eq.getArg2()).getAsNode());
					if (ci2 != null)
					{
						QueryItemName ci1a = isMapped( ci1 );
						QueryItemName ci2a = isMapped( ci2 );
						if ((ci1a != null && ci2a == null) ||
								(ci1a == null && ci2a != null))
						{
							Node n = null;
							// equals and one of the columns is aliased.
							if (ci1a != null)
							{
								// first one is aliased
								n= addAlias( ci1, ci1a );
							}
							else
							{
								n = addAlias( ci2, ci2a );
							}
							stack.push( new E_Bound( new ExprVar( n )) );
							return;
						}
					}
				}
			}
		}
		
		pushArgs( func );
		stack.push( func.copy( stack.pop(), stack.pop() ));
	}

	@Override
	public void visit( ExprFunction3 func )
	{
		pushArgs( func );
		stack.push( func.copy( stack.pop(), stack.pop(), stack.pop() ));
	}
	
	private ExprList createExprList( ExprFunction expr )
	{
		pushArgs( expr );
		ExprList lst = new ExprList();
		for (int i=0;i<expr.numArgs();i++)
		{
			lst.add( stack.pop() );
		}
		return lst;
	}

	@Override
	public void visit( ExprFunctionN func )
	{	
		try {
			  Method m = ExprFunctionN.class.getMethod("copy",ExprList.class);
			   m.setAccessible(true);
			   stack.push( (Expr) m.invoke( func, createExprList( func ) ));
		}
		catch (NoSuchMethodException e)
		{
			throw new IllegalStateException( String.format( "%s copy(ExprList) method is required", func.getClass()), e );
		}
		catch (SecurityException e)
		{
			throw new IllegalStateException( String.format( "Could not make %s copy(ExprList) method visible", func.getClass()), e );
		}
		catch (IllegalAccessException e)
		{
			throw new IllegalStateException( String.format( "Could not make %s copy(ExprList) method visible", func.getClass()), e );
		}
		catch (InvocationTargetException e)
		{
			throw new IllegalStateException( String.format( "Could not invoke %s copy(ExprList) method", func.getClass()), e );
		}
	}

	@Override
	public void visit( ExprFunctionOp funcOp )
	{
		stack.push( funcOp.copy( createExprList( funcOp ), funcOp.getGraphPattern() ));
	}

	@Override
	public void visit( NodeValue nv )
	{
		stack.push( nv );
	}

	@Override
	public void visit( ExprVar nv )
	{
		Node n = ((ExprVar) nv).getAsNode();
//		final QueryTableInfo sti = queryBuilder.getNodeTable(n);
//		if (sti != null) {
//			if (sti.isOptional())
//		
//		{
//			sti.addFilter(toApply);
//			return true;
//		}
//			else
//			{
//				return false;
//			}
//		}
//		else {
			final QueryColumnInfo ci = queryBuilder.getNodeColumn(n);
			if (ci != null)
			{
				for (QueryItemName qi : aliasMap.keySet())
				{
					if (QueryColumnInfo.getNameInstance(qi).matches( ci.getName()))
					{
						QueryItemName mapTo = aliasMap.get(qi);
						mapTo = QueryColumnInfo.getNameInstance(mapTo.getSchema(), mapTo.getTable(), ci.getName().getCol());
						stack.push(new ExprVar( mapTo.getSPARQLName() ));
						return;
					}
				}
			}
			stack.push( nv );
	}

	@Override
	public void visit( ExprAggregator eAgg )
	{
		Aggregator agg = eAgg.getAggregator();
		Expr exp = agg.getExpr();
		if (exp != null)
		{
			exp.visit(this);
			exp = stack.pop();
		}
		stack.push( new ExprAggregator( eAgg.getVar(), agg.copy(exp)));		
	}

	@Override
	public void finishVisit()
	{	
	}

}
