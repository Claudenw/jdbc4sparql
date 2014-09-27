package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionEnv;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;

/**
 * A local filter that removes any values that are null and not allowed to
 * be null or that can not be converted
 * to the expected column value type.
 */
public class CheckTypeF extends ExprFunction1
{
	private final Column column;

	public CheckTypeF( final Column column, final Var columnVar )
	{
		super(new ExprVar(columnVar), "checkTypeF");
		if (column == null)
		{
			throw new IllegalArgumentException("Column may not be null");
		}
		this.column = column;
	}

	@Override
	public Expr copy( final Expr expr )
	{
		return new CheckTypeF(column, expr.asVar());
	}

	@Override
	public boolean equals( final Object o )
	{
		if (o instanceof CheckTypeF)
		{
			final CheckTypeF cf = (CheckTypeF) o;

			return column.equals(cf.column)
					&& getArg().asVar().equals(cf.getArg().asVar());
		}
		return false;
	}

	@Override
	public NodeValue eval( final NodeValue v )
	{
		return NodeValue.FALSE;
	}

	@Override
	protected NodeValue evalSpecial( final Binding binding,
			final FunctionEnv env )
	{
		final Var v = expr.asVar();
		final Node n = binding.get(v);
		if (n == null)
		{
			boolean b = column.getColumnDef().getNullable() == ResultSetMetaData.columnNullable;
			return column.getColumnDef().getNullable() == ResultSetMetaData.columnNullable ? NodeValue.TRUE
					: NodeValue.FALSE;
		}
		final Class<?> resultingClass = TypeConverter.getJavaType(column
				.getColumnDef().getType());
		Object columnObject;
		if (n.isLiteral())
		{
			columnObject = n.getLiteralValue();
		}
		else if (n.isURI())
		{
			columnObject = n.getURI();
		}
		else if (n.isBlank())
		{
			columnObject = n.getBlankNodeId().toString();
		}
		else if (n.isVariable())
		{
			columnObject = n.getName();
		}
		else
		{
			columnObject = n.toString();
		}

		try
		{
			AbstractResultSet.extractData(columnObject, resultingClass);
			return NodeValue.TRUE;
		}
		catch (final SQLException e)
		{
			return NodeValue.FALSE;
		}

	}
}