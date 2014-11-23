package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Stack;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

public class SystemFunctionHandler extends AbstractFunctionHandler {
	public static final String[] SYSTEM_FUNCTIONS = { "CATALOG", "VERSION" };
	private static final int CATALOG = 0;
	private static final int VERSION = 1;

	public SystemFunctionHandler(final SparqlQueryBuilder builder,
			final Stack<Expr> stack) {
		super(builder, stack);
	}

	@Override
	public ColumnDef handle(final Function func) throws SQLException {
		ColumnDef retval = null;
		QueryColumnInfo qci = null;
		final int stackCheck = stack.size();
		final int i = Arrays.asList(SystemFunctionHandler.SYSTEM_FUNCTIONS)
				.indexOf(func.getName().toUpperCase());
		ColumnName colName;
		NodeValueString str;
		switch (i) {
		case CATALOG:
			str = new NodeValueString(builder.getCatalog().getName()
					.getShortName());
			stack.push(str);
			colName = tblName.getColumnName(func.getName());
			builder.registerFunction(colName, java.sql.Types.VARCHAR);
			builder.addVar(str, colName);
			qci = builder.getColumn(colName);
			retval = qci.getColumn().getColumnDef();
			break;
		case VERSION:
			str = new NodeValueString(J4SDriver.getVersion());
			stack.push(str);
			colName = tblName.getColumnName(func.getName());
			builder.registerFunction(colName, java.sql.Types.VARCHAR);
			builder.addVar(str, colName);
			qci = builder.getColumn(colName);
			retval = qci.getColumn().getColumnDef();
			break;
		default:
			return null;
		}
		if (stack.size() != (stackCheck + 1)) {
			throw new IllegalStateException(String.format(
					"Expected %s items on stack, found %s", stackCheck + 1,
					stack.size()));
		}
		return retval;
	}

}
