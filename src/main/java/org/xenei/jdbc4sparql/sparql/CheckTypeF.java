package org.xenei.jdbc4sparql.sparql;

import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionEnv;

/**
 * A local filter that removes any values that are null and not allowed to be
 * null or that can not be converted to the expected column value type.
 */
public class CheckTypeF extends ExprFunction1 {
	private final QueryColumnInfo columnInfo;
	private final Class<?> resultingClass;
	private Object convertedValue;
	private static final Logger LOG = LoggerFactory.getLogger(CheckTypeF.class);

	private static QueryColumnInfo checkColumnInfo(
			final QueryColumnInfo columnInfo) {
		if (columnInfo == null) {
			throw new IllegalArgumentException("Column may not be null");
		}
		return columnInfo;
	}

	public CheckTypeF(final QueryColumnInfo columnInfo) throws SQLDataException {
		super(new ExprVar(checkColumnInfo(columnInfo).getGUIDVar()),
				"checkTypeF");

		this.columnInfo = columnInfo;
		resultingClass = TypeConverter.getJavaType(columnInfo.getColumn()
				.getColumnDef().getType());
	}

	@Override
	public Expr copy(final Expr expr) {
		try {
			return new CheckTypeF(columnInfo);
		} catch (final SQLDataException e) {
			throw new IllegalStateException(String.format(
					"Error while copying: %s", e.getMessage()), e);
		}
	}

	@Override
	public boolean equals(final Object o) {
		if (o instanceof CheckTypeF) {
			final CheckTypeF cf = (CheckTypeF) o;

			return columnInfo.equals(cf.columnInfo)
					&& getArg().asVar().equals(cf.getArg().asVar());
		}
		return false;
	}

	@Override
	public NodeValue eval(final NodeValue v) {
		return NodeValue.FALSE;
	}

	@Override
	protected NodeValue evalSpecial(final Binding binding, final FunctionEnv env) {
		final Var v = expr.asVar();
		final Node n = binding.get(v);
		if (n == null) {
			final boolean retval = columnInfo.getColumn().getColumnDef()
					.getNullable() == ResultSetMetaData.columnNullable;
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} ({}) of {} ", this, columnInfo.getName(), binding);
				LOG.debug("with value  {} is {}", n, retval);
			}
			return retval ? NodeValue.TRUE : NodeValue.FALSE;
		}
		Object columnObject;
		if (n.isLiteral()) {
			columnObject = n.getLiteralValue();
		}
		else if (n.isURI()) {
			columnObject = n.getURI();
		}
		else if (n.isBlank()) {
			columnObject = n.getBlankNodeId().toString();
		}
		else if (n.isVariable()) {
			columnObject = n.getName();
		}
		else {
			columnObject = n.toString();
		}

		try {
			convertedValue = TypeConverter.extractData(columnObject,
					resultingClass);
			boolean retval = true;
			if (convertedValue == null) {
				retval = columnInfo.getColumn().getColumnDef().getNullable() == ResultSetMetaData.columnNullable;
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} ({}) of {} ", this, columnInfo.getName(), binding);
				LOG.debug("with value ({}) {} is {}", n, convertedValue, retval);
			}
			return retval ? NodeValue.TRUE : NodeValue.FALSE;

		} catch (final SQLException e) {
			return NodeValue.FALSE;
		}
	}

	public Object getValue() {
		return convertedValue;
	}

	public QueryColumnInfo getColumnInfo() {
		return columnInfo;
	}

}