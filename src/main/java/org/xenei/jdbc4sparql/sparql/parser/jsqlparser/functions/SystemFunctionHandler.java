package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.ExprColumn;

import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

public class SystemFunctionHandler extends AbstractFunctionHandler {
	public static final String[] SYSTEM_FUNCTIONS = {
			"CATALOG", "VERSION"
	};
	private static final int CATALOG = 0;
	private static final int VERSION = 1;

	public SystemFunctionHandler(final SparqlQueryBuilder builder) {
		super(builder);
	}

	@Override
	public FuncInfo handle(final Function func, final String alias)
			throws SQLException {

		final int i = Arrays.asList(SystemFunctionHandler.SYSTEM_FUNCTIONS)
				.indexOf(func.getName().toUpperCase());
		ColumnName colName;
		NodeValueString str;
		switch (i) {
			case CATALOG:
				str = new NodeValueString(builder.getCatalog().getName()
						.getShortName());
				// colName = tblName.getColumnName(func.getName());
				colName = tblName.getColumnName(alias);
				builder.registerFunction(colName, java.sql.Types.VARCHAR);
				return new FuncInfo(str, colName,
						Collections.<ExprColumn> emptySet());

			case VERSION:
				str = new NodeValueString(J4SDriver.getVersion());
				// colName = tblName.getColumnName(func.getName());
				colName = tblName.getColumnName(alias);
				builder.registerFunction(colName, java.sql.Types.VARCHAR);
				return new FuncInfo(str, colName,
						Collections.<ExprColumn> emptySet());
			default:
				return null;
		}

	}

}
