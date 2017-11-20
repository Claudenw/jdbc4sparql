package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import java.sql.SQLException;
import java.util.Arrays;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.SparqlExprVisitor.AliasInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfoFactory;

import org.apache.jena.sparql.expr.E_StrLength;
import org.apache.jena.sparql.expr.E_StrLowerCase;
import org.apache.jena.sparql.expr.E_StrReplace;
import org.apache.jena.sparql.expr.E_StrSubstring;
import org.apache.jena.sparql.expr.E_StrUpperCase;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;

public class StringFunctionHandler extends AbstractFunctionHandler {
	public static final String[] STRING_FUNCTIONS = {
			"LENGTH", "SUBSTRING", "UCASE", "LCASE", "REPLACE"
	};
	private static final int LENGTH = 0;
	private static final int SUBSTRING = 1;
	private static final int UCASE = 2;
	private static final int LCASE = 3;
	private static final int REPLACE = 4;

	public StringFunctionHandler(final SparqlQueryBuilder builder) {
		super(builder);
	}

	private String escapeRegex(final NodeValueString pattern) {
		final String chars = "\\.^$*+?()[{|";
		final StringBuilder sb = new StringBuilder();
		for (final char c : pattern.asUnquotedString().toCharArray()) {
			if (chars.indexOf(c) > -1) {
				sb.append("\\");
			}
			sb.append(c);
		}
		return sb.toString();
	}

	@Override
	public Expr handle(final Function func, final AliasInfo alias)
			throws SQLException {
		int i = Arrays.asList(StringFunctionHandler.STRING_FUNCTIONS).indexOf(
				func.getName().toUpperCase());
		if (i == -1) {
			final String s = func.getName().toUpperCase();
			if ("LEN".equals(s)) {
				i = StringFunctionHandler.LENGTH;
			}
			else if ("UPPER".equals(s)) {
				i = StringFunctionHandler.UCASE;
			}
			else if ("LOWER".equals(s)) {
				i = StringFunctionHandler.LCASE;
			}
		}
		switch (i) {
			case LENGTH:
				return handleExpr1(E_StrLength.class, func,
						java.sql.Types.VARCHAR, alias);

			case SUBSTRING:
				return handleSubString(func, alias);

			case UCASE:
				return handleExpr1(E_StrUpperCase.class, func,
						java.sql.Types.VARCHAR, alias);

			case LCASE:
				return handleExpr1(E_StrLowerCase.class, func,
						java.sql.Types.VARCHAR, alias);

			case REPLACE:
				return handleReplace(func, alias);

			default:
				return null;
		}
	}

	protected Expr handleReplace(final Function func, final AliasInfo alias)
			throws SQLException {

		final ExpressionList l = func.getParameters();
		if (l == null) {
			throw getNoArgumentEx(func, "three");
		}
		if (l.getExpressions().size() != 3) {
			throw getToManyArgumentEx(func, "three");
		}

		// third param
		((Expression) l.getExpressions().get(2)).accept(exprVisitor);
		Expr arg3 = exprVisitor.getResult();
		if (arg3 instanceof NodeValueString) {
			arg3 = new NodeValueString(escapeRegex((NodeValueString) arg3));
		}
		else {
			throw new IllegalArgumentException("parameter 3 must be a string");
		}

		// second param
		((Expression) l.getExpressions().get(1)).accept(exprVisitor);
		Expr arg2 = exprVisitor.getResult();
		if (arg2 instanceof NodeValueString) {
			arg2 = new NodeValueString(escapeRegex((NodeValueString) arg2));
		}
		else {
			throw new IllegalArgumentException("parameter 2 must be a string");
		}

		// first param
		((Expression) l.getExpressions().get(0)).accept(exprVisitor);
		final Expr arg1 = exprVisitor.getResult();

		final Expr expr = new E_StrReplace(arg1, arg2, arg3, null);
		// final ColumnName colName = tblName.getColumnName(func.getName());
		final ColumnName colName = tblName.getColumnName(alias.getAlias());
		builder.registerFunction(colName, java.sql.Types.VARCHAR);
		return ExprInfoFactory.getInstance(expr, exprVisitor.getColumns(),
				colName);
	}

	protected Expr handleSubString(final Function func, final AliasInfo alias)
			throws SQLException {
		final ExpressionList l = func.getParameters();
		Expr expr3 = null;
		Expr expr2 = null;
		Expr expr1 = null;
		if (l == null) {
			throw getNoArgumentEx(func, "two or three");
		}
		final int count = l.getExpressions().size();
		if (count < 2) {
			throw this.getWrongArgumentCountEx(func, "two or three", count);
		}
		if (count > 3) {
			throw getToManyArgumentEx(func, "two or three");
		}

		// push expressions in reverse order.
		// third param
		if (count == 3) {
			((Expression) l.getExpressions().get(2)).accept(exprVisitor);
			expr3 = exprVisitor.getResult();
		}
		// second param
		((Expression) l.getExpressions().get(1)).accept(exprVisitor);
		expr2 = exprVisitor.getResult();

		// first param
		((Expression) l.getExpressions().get(0)).accept(exprVisitor);
		expr1 = exprVisitor.getResult();

		final Expr expr = new E_StrSubstring(expr1, expr2, expr3);
		// final ColumnName colName = tblName.getColumnName(func.getName());
		final ColumnName colName = tblName.getColumnName(alias.getAlias());
		builder.registerFunction(colName, java.sql.Types.VARCHAR);
		return ExprInfoFactory.getInstance(expr, exprVisitor.getColumns(),
				colName);

	}
}
