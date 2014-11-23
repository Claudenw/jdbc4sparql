package org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions;

import com.hp.hpl.jena.sparql.expr.E_StrLength;
import com.hp.hpl.jena.sparql.expr.E_StrLowerCase;
import com.hp.hpl.jena.sparql.expr.E_StrReplace;
import com.hp.hpl.jena.sparql.expr.E_StrSubstring;
import com.hp.hpl.jena.sparql.expr.E_StrUpperCase;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;

public class StringFunctionHandler extends AbstractFunctionHandler {
	public static final String[] STRING_FUNCTIONS = { "LENGTH", "SUBSTRING",
			"UCASE", "LCASE", "REPLACE" };
	private static final int LENGTH = 0;
	private static final int SUBSTRING = 1;
	private static final int UCASE = 2;
	private static final int LCASE = 3;
	private static final int REPLACE = 4;

	public StringFunctionHandler(final SparqlQueryBuilder builder,
			final Stack<Expr> stack) {
		super(builder, stack);
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
	public ColumnDef handle(final Function func) throws SQLException {
		ColumnDef retval = null;
		final int stackCheck = stack.size();
		int i = Arrays.asList(StringFunctionHandler.STRING_FUNCTIONS).indexOf(
				func.getName().toUpperCase());
		if (i == -1) {
			final String s = func.getName().toUpperCase();
			if ("LEN".equals(s)) {
				i = StringFunctionHandler.LENGTH;
			} else if ("UPPER".equals(s)) {
				i = StringFunctionHandler.UCASE;
			} else if ("LOWER".equals(s)) {
				i = StringFunctionHandler.LCASE;
			}
		}
		switch (i) {
		case LENGTH:
			retval = handleExpr1(E_StrLength.class, func,
					java.sql.Types.VARCHAR);
			break;
		case SUBSTRING:
			retval = handleSubString(func);
			break;
		case UCASE:
			retval = handleExpr1(E_StrUpperCase.class, func,
					java.sql.Types.VARCHAR);
			break;
		case LCASE:
			retval = handleExpr1(E_StrLowerCase.class, func,
					java.sql.Types.VARCHAR);
			break;
		case REPLACE:
			retval = handleReplace(func);
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

	protected ColumnDef handleReplace(final Function func) throws SQLException {
		final ExpressionList l = func.getParameters();
		if (l == null) {
			throw getNoArgumentEx(func, "three");
		}
		if (l.getExpressions().size() != 3) {
			throw getToManyArgumentEx(func, "three");
		}

		// push expressions in reverse order.
		// third param
		((Expression) l.getExpressions().get(2)).accept(exprVisitor);
		Expr expr = exprVisitor.getResult();
		if (expr instanceof NodeValueString) {

			expr = new NodeValueString(escapeRegex((NodeValueString) expr));
			stack.push(expr);
		} else {
			throw new IllegalArgumentException("parameter 3 must be a string");
		}

		// second param
		((Expression) l.getExpressions().get(1)).accept(exprVisitor);
		expr = exprVisitor.getResult();
		if (expr instanceof NodeValueString) {
			expr = new NodeValueString(escapeRegex((NodeValueString) expr));
			stack.push(expr);
		} else {
			throw new IllegalArgumentException("parameter 2 must be a string");
		}

		// first param
		((Expression) l.getExpressions().get(0)).accept(exprVisitor);
		stack.push(exprVisitor.getResult());

		// stack.push(new E_StrReplace(stack.pop(), stack.pop(), stack.pop(),
		// null));
		expr = new E_StrReplace(stack.pop(), stack.pop(), stack.pop(), null);
		stack.push(expr);
		final ColumnName colName = tblName.getColumnName(func.getName());
		builder.registerFunction(colName, java.sql.Types.VARCHAR);
		builder.addVar(expr, colName);
		QueryColumnInfo qci = builder.getColumn(colName);
		return qci.getColumn().getColumnDef();
	}

	protected ColumnDef handleSubString(final Function func)
			throws SQLException {
		final ExpressionList l = func.getParameters();

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
			stack.push(exprVisitor.getResult());
		}
		// second param
		((Expression) l.getExpressions().get(1)).accept(exprVisitor);
		stack.push(exprVisitor.getResult());

		// first param
		((Expression) l.getExpressions().get(0)).accept(exprVisitor);
		stack.push(exprVisitor.getResult());

		final Expr expr = new E_StrSubstring(stack.pop(), stack.pop(),
				count == 3 ? stack.pop() : null);
		stack.push(expr);
		final ColumnName colName = tblName.getColumnName(func.getName());
		builder.registerFunction(colName, java.sql.Types.VARCHAR);
		builder.addVar(expr, colName);
		QueryColumnInfo qci = builder.getColumn(colName);
		return qci.getColumn().getColumnDef();
	}
}
