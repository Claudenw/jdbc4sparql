package org.xenei.jdbc4sparql.sparql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprEvalException;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueBoolean;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.function.Function;
import com.hp.hpl.jena.sparql.function.FunctionEnv;

/**
 * A local filter that removes any values that are null and not allowed to be
 * null or that can not be converted to the expected column value type.
 */
public class CheckTypeF implements Function {
	private ExprVar var;
	private NodeValueInteger type;
	private NodeValueBoolean nullable;
	private Object convertedValue;
	private static final Logger LOG = LoggerFactory.getLogger(CheckTypeF.class);
	private static final String IRI = "java:"
			+ CheckTypeF.class.getCanonicalName();

	protected static ExprList getExprList(final QueryColumnInfo columnInfo) {
		if (columnInfo == null) {
			throw new IllegalArgumentException("ColumnInfo may not be null");
		}
		final ExprList args = new ExprList();

		args.add(new ExprVar(columnInfo.getGUIDVar()));
		args.add(NodeValue.makeInteger(columnInfo.getColumn().getColumnDef()
				.getType()));
		args.add(NodeValue.booleanReturn(columnInfo.getColumn().getColumnDef()
				.getNullable() == ResultSetMetaData.columnNullable));
		return args;
	}

	public static E_Function getFunction(final QueryColumnInfo columnInfo) {
		return new E_Function(IRI, getExprList(columnInfo));
	}

	public CheckTypeF() {
	}

	private void configure(final Expr arg1, final Expr arg2, final Expr arg3) {
		if (!(arg1 instanceof ExprVar)) {
			throw new IllegalArgumentException("Argument 1 must be a ExprVar");
		}
		var = (ExprVar) arg1;

		if (!(arg2 instanceof NodeValueInteger)) {
			throw new IllegalArgumentException(
					"Argument 2 must be a NodeValueInteger");
		}
		type = (NodeValueInteger) arg2;

		if (!(arg3 instanceof NodeValueBoolean)) {
			throw new IllegalArgumentException(
					"Argument 3 must be a NodeValueBoolean");
		}
		nullable = (NodeValueBoolean) arg3;
	}

	public Object getValue() {
		return convertedValue;
	}

	@Override
	public void build(final String uri, final ExprList args) {
		if (args.size() != 3) {
			throw new IllegalArgumentException("There must be 3 arguments to "
					+ this.getClass().getSimpleName());
		}
		configure(args.get(0), args.get(1), args.get(2));
	}

	@Override
	public NodeValue exec(final Binding binding, final ExprList args,
			final String uri, final FunctionEnv env) {

		if (!var.equals(args.get(0))) {
			throw new ExprEvalException(
					String.format("arg 1 (%s) does not match expected (%s)",
							args.get(0), var));
		}

		if (!type.equals(args.get(1))) {
			throw new ExprEvalException(String.format(
					"arg 2 (%s) does not match expected (%s)", args.get(1),
					type));
		}

		if (!nullable.equals(args.get(2))) {
			throw new ExprEvalException(String.format(
					"arg 3 (%s) does not match expected (%s)", args.get(2),
					nullable));
		}

		final int sqlType = type.getInteger().intValue();

		final Node n = binding.get(var.asVar());
		if (n == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} with value {} is {}", var, n, nullable);
			}
			return nullable;
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
					TypeConverter.getJavaType(sqlType));
			boolean retval = true;
			if (convertedValue == null) {
				retval = nullable.getBoolean();
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("{} ({}) of {} ", this, var, binding);
				LOG.debug("with value ({}) {} is {}", n, convertedValue, retval);
			}
			return retval ? NodeValue.TRUE : NodeValue.FALSE;

		} catch (final SQLException e) {
			return NodeValue.FALSE;
		}
	}

}