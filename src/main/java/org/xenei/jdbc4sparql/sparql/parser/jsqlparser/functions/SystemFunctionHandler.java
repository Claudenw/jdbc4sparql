package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.sql.SQLException;
import java.util.Arrays;

import net.sf.jsqlparser.expression.Function;

import org.xenei.jdbc4sparql.J4SDriver;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfoFactory;

import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;

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
	public Expr handle(final Function func, final AliasInfo alias)
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
				colName = getColumnName( alias );
				builder.registerFunction(colName, java.sql.Types.VARCHAR);
				return ExprInfoFactory.getInstance(str, colName);

			case VERSION:
				str = new NodeValueString(J4SDriver.getVersion());
				// colName = tblName.getColumnName(func.getName());
				colName = getColumnName( alias );
				builder.registerFunction(colName, java.sql.Types.VARCHAR);
				return ExprInfoFactory.getInstance(str, colName);
			default:
				return null;
		}

	}

}
