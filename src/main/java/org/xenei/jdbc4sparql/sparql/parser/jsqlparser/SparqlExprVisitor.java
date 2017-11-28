/*
 * This file is part of jdbc4sparql jsqlparser implementation.
 *
 * jdbc4sparql jsqlparser implementation is free software: you can redistribute
 * it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * jdbc4sparql jsqlparser implementation is distributed in the hope that it will
 * be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jdbc4sparql jsqlparser implementation. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.iface.name.NameSegments;
import org.xenei.jdbc4sparql.impl.virtual.VirtualSchema;
import org.xenei.jdbc4sparql.impl.virtual.VirtualTable;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.proxies.ExprInfoFactory;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.E_Bound;
import org.apache.jena.sparql.expr.E_Divide;
import org.apache.jena.sparql.expr.E_Equals;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual;
import org.apache.jena.sparql.expr.E_LessThan;
import org.apache.jena.sparql.expr.E_LessThanOrEqual;
import org.apache.jena.sparql.expr.E_LogicalAnd;
import org.apache.jena.sparql.expr.E_LogicalNot;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_Multiply;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.E_OneOf;
import org.apache.jena.sparql.expr.E_Regex;
import org.apache.jena.sparql.expr.E_Subtract;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDT;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;

/**
 * An expression visitor. Merges SQL expressions into the SparqlQueryBuilder.
 */
public class SparqlExprVisitor implements ExpressionVisitor {
	// the query builder
	private final SparqlQueryBuilder builder;

	// A stack of expression elements.
	private final Stack<Expr> stack;

	private final Set<ExprColumn> columns;

	private final boolean optionalColumns;

	private final AliasInfo alias;

	private static Logger LOG = LoggerFactory
			.getLogger(SparqlExprVisitor.class);

	/**
	 * Constructor
	 *
	 * @param builder
	 *            The SparqlQueryBuilder to use.
	 */
	public SparqlExprVisitor(final SparqlQueryBuilder builder,
			final boolean optionalColumns, final boolean aliasRequired,
			final String alias) {
		this.builder = builder;
		this.optionalColumns = optionalColumns;
		this.alias = new AliasInfo(alias, aliasRequired);
		stack = new Stack<Expr>();
		columns = new HashSet<ExprColumn>();
	}

	/**
	 * Constructor
	 *
	 * @param builder
	 *            The SparqlQueryBuilder to use.
	 */
	public SparqlExprVisitor(final SparqlQueryBuilder builder,
			final boolean optionalColumns, final boolean aliasRequired) {
		this(builder, optionalColumns, aliasRequired, null);
	}

	public Set<ExprColumn> getColumns() {
		return columns;
	}

	/**
	 * Get the final result of the process.
	 *
	 * @return
	 */
	public Expr getResult() {
		return stack.pop();
	}

	/**
	 *
	 * @return True if the stack is empty (no result).
	 */
	public boolean isEmpty() {
		return stack.isEmpty();
	}

	private void logVisit(final String part, final Expression name) {
		final String aliasLog = alias == null ? "" : String.format(" as %s",
				alias);
		SparqlExprVisitor.LOG.debug("visit {}: {}{}", part, name, aliasLog);
	}

	// process a binary expression.
	private void process(final BinaryExpression biExpr) {
		// put on in reverse order so they can be popped back off in the proper
		// order.
		biExpr.getRightExpression().accept(this);
		biExpr.getLeftExpression().accept(this);
	}

	private AliasInfo getAlias() {
		if (alias.isUsed()) {
			return alias;
		}
		// must copy default alias not calculated alias.
		final AliasInfo retval = new AliasInfo(alias.alias,
				alias.isAliasRequired());
		alias.setUsed(true);
		alias.aliasRequired = false;
		return retval;
	}

	private Expr processAlias(final Expr expr, final AliasInfo alias) {
		if (alias.isAliasRequired()) {
			final ColumnName cName = new ColumnName(builder.getCatalogName(),
					VirtualSchema.NAME, VirtualTable.NAME, alias.getAlias());
			return ExprInfoFactory.getInstance(expr, columns, cName);

		}
		return expr;
	}

	@Override
	public void visit(final Addition addition) {
		if (LOG.isDebugEnabled()) {
			logVisit("Addition", addition);
		}
		final AliasInfo exprAlias = getAlias();
		process(addition);
		stack.push(processAlias(new E_Add(stack.pop(), stack.pop()), exprAlias));
	}

	@Override
	public void visit(final AllComparisonExpression allComparisonExpression) {
		throw new UnsupportedOperationException("ALL is not supported");
	}

	@Override
	public void visit(final AndExpression andExpression) {
		if (LOG.isDebugEnabled()) {
			logVisit("And", andExpression);
		}
		final AliasInfo exprAlias = getAlias();
		process(andExpression);
		stack.push(processAlias(new E_LogicalAnd(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final AnyComparisonExpression anyComparisonExpression) {
		throw new UnsupportedOperationException("ANY is not supported");
	}

	@Override
	public void visit(final Between between) {
		if (LOG.isDebugEnabled()) {
			logVisit("Between", between);
		}
		final AliasInfo exprAlias = getAlias();
		between.getBetweenExpressionEnd().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getLeftExpression().accept(this);
		// rewrite as x <= a >= y
		final Expr a = stack.pop();
		final Expr left = new E_LessThanOrEqual(stack.pop(), a);
		final Expr right = new E_GreaterThanOrEqual(a, stack.pop());
		stack.push(processAlias(new E_LogicalAnd(left, right), exprAlias));
	}

	@Override
	public void visit(final BitwiseAnd bitwiseAnd) {
		throw new UnsupportedOperationException("'&' is not supported");
	}

	@Override
	public void visit(final BitwiseOr bitwiseOr) {
		throw new UnsupportedOperationException("'|' is not supported");
	}

	@Override
	public void visit(final BitwiseXor bitwiseXor) {
		throw new UnsupportedOperationException("'^' is not supported");
	}

	@Override
	public void visit(final CaseExpression caseExpression) {
		throw new UnsupportedOperationException("CASE is not supported");
	}

	@Override
	public void visit(final Column tableColumn) {
		if (LOG.isDebugEnabled()) {
			logVisit("Column", tableColumn);
		}
		final String schema = StringUtils.defaultString(tableColumn.getTable()
				.getSchemaName(), builder.getDefaultSchemaName());
		final String table = StringUtils.defaultString(tableColumn.getTable()
				.getName(), builder.getDefaultTableName());
		QueryColumnInfo columnInfo = null;
		QueryTableInfo tableInfo = null;
		if ((table == null) || (schema == null)) {
			final ColumnName cName = new ColumnName(builder.getCatalogName(),
					StringUtils.defaultString(schema),
					StringUtils.defaultString(table),
					tableColumn.getColumnName());
			// find the column by name -- must only be one or it throws an
			// exception.
			columnInfo = builder.getColumn(cName.clone(NameSegments.FFFT));
		}
		else {
			// column name will not be in infoSet yet if we are processing a
			// join.
			final ColumnName cName = new ColumnName(builder.getCatalogName(),
					schema, table, tableColumn.getColumnName());
			try {
				columnInfo = builder.addColumn(cName, optionalColumns);
			} catch (final SQLException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
		}
		tableInfo = builder.getTable(columnInfo.getName().getTableName());
		final AliasInfo exprAlias = getAlias();
		if (exprAlias.isAliasRequired() && !exprAlias.isUsed()) {
			final ColumnName aliasName = ColumnName.getNameInstance(
					columnInfo.getName(), exprAlias.getAlias());
			columnInfo = tableInfo.addColumnToQuery(columnInfo.getColumn(),
					aliasName, optionalColumns);
		}
		else {
			columnInfo = tableInfo.addColumnToQuery(columnInfo.getColumn(),
					optionalColumns);
		}
		final ExprColumn column = new ExprColumn(columnInfo);
		columns.add(column);
		stack.push(column);
	}

	@Override
	public void visit(final Concat concat) {
		throw new UnsupportedOperationException("CONCAT is not supported");
	}

	@Override
	public void visit(final DateValue dateValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("DateValue", dateValue);
		}
		final String val = dateValue.getValue().toString();
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDdate);
		stack.push(processAlias(new NodeValueDT(val, n), getAlias()));
	}

	@Override
	public void visit(final Division division) {
		if (LOG.isDebugEnabled()) {
			logVisit("Division", division);
		}
		final AliasInfo exprAlias = getAlias();
		process(division);
		stack.push(processAlias(new E_Divide(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final DoubleValue doubleValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("DoubleValue", doubleValue);
		}
		stack.push(processAlias(new NodeValueDouble(doubleValue.getValue()),
				getAlias()));
	}

	@Override
	public void visit(final EqualsTo equalsTo) {
		if (LOG.isDebugEnabled()) {
			logVisit("EqualsTo", equalsTo);
		}
		final AliasInfo exprAlias = getAlias();
		process(equalsTo);
		stack.push(processAlias(new E_Equals(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final ExistsExpression existsExpression) {
		throw new UnsupportedOperationException("EXISTS is not supported");
	}

	@Override
	public void visit(final Function function) {
		if (LOG.isDebugEnabled()) {
			logVisit("Function", function);
		}
		final StandardFunctionHandler sfh = new StandardFunctionHandler(builder);
		try {

			final Expr exprInfo = sfh.handle(function, getAlias());
			stack.push(exprInfo);
		} catch (final SQLException e) {
			throw new UnsupportedOperationException(String.format(
					"function %s is not supported (%s)", function.getName(),
					e.getMessage()));
		}
	}

	@Override
	public void visit(final GreaterThan greaterThan) {
		if (LOG.isDebugEnabled()) {
			logVisit("GreaterThan", greaterThan);
		}
		final AliasInfo exprAlias = getAlias();
		process(greaterThan);
		stack.push(processAlias(new E_GreaterThan(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final GreaterThanEquals greaterThanEquals) {
		if (LOG.isDebugEnabled()) {
			logVisit("GreaterThanEquals", greaterThanEquals);
		}
		final AliasInfo exprAlias = getAlias();
		process(greaterThanEquals);
		stack.push(processAlias(
				new E_GreaterThanOrEqual(stack.pop(), stack.pop()), exprAlias));
	}

	@Override
	public void visit(final InExpression inExpression) {
		if (LOG.isDebugEnabled()) {
			logVisit("InExpression", inExpression);
		}
		final AliasInfo exprAlias = getAlias();
		final SparqlItemsListVisitor listVisitor = new SparqlItemsListVisitor(
				builder);
		inExpression.getItemsList().accept(listVisitor);
		inExpression.getLeftExpression().accept(this);
		stack.push(processAlias(
				new E_OneOf(stack.pop(), listVisitor.getResult()), exprAlias));
	}

	@Override
	public void visit(final InverseExpression inverseExpression) {
		throw new UnsupportedOperationException(
				"inverse expressions are not supported");
	}

	@Override
	public void visit(final IsNullExpression isNullExpression) {
		if (LOG.isDebugEnabled()) {
			logVisit("isNull", isNullExpression);
		}
		final AliasInfo exprAlias = getAlias();
		isNullExpression.getLeftExpression().accept(this);
		ExprFunction func = null;
		final Expr expr = stack.pop();
		if (expr instanceof ExprColumn) {
			func = new E_Bound(new ExprVar(((ExprColumn) expr).getColumnInfo()
					.getGUIDVar()));
		}
		else {
			func = new E_Bound(expr);
		}

		// SQL has is NULL = T when empty
		// SPARQL has bound()= F when empty
		// so this NOT check looks backward at first.
		if (!isNullExpression.isNot()) {
			func = new E_LogicalNot(func);

		}
		stack.push(processAlias(func, exprAlias));
	}

	@Override
	public void visit(final JdbcParameter jdbcParameter) {
		throw new UnsupportedOperationException(
				"JDBC Parameters are not supported");
	}

	@Override
	public void visit(final LikeExpression likeExpression) {
		if (LOG.isDebugEnabled()) {
			logVisit("LikeExpression", likeExpression);
		}
		final AliasInfo exprAlias = getAlias();
		process(likeExpression);
		final Expr left = stack.pop();
		final Expr right = stack.pop();
		if (right instanceof NodeValueString) {
			final RegexNodeValue rnv = RegexNodeValue
					.create(((NodeValueString) right).getString());
			if (rnv.isWildcard()) {
				stack.push(processAlias(new E_Regex(left, rnv,
						new NodeValueString("")), exprAlias));
			}
			else {
				stack.push(processAlias(new E_Equals(left, rnv), exprAlias));
			}
		}
		else {
			throw new UnsupportedOperationException(
					"LIKE must have string for right hand argument");
		}
	}

	@Override
	public void visit(final LongValue longValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("Long", longValue);
		}
		stack.push(processAlias(new NodeValueInteger(longValue.getValue()),
				getAlias()));
	}

	@Override
	public void visit(final Matches matches) {
		throw new UnsupportedOperationException("MATCHES is not supported");
	}

	@Override
	public void visit(final MinorThan minorThan) {
		if (LOG.isDebugEnabled()) {
			logVisit("MinorThan", minorThan);
		}
		final AliasInfo exprAlias = getAlias();
		process(minorThan);
		stack.push(processAlias(new E_LessThan(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final MinorThanEquals minorThanEquals) {
		if (LOG.isDebugEnabled()) {
			logVisit("MinorThanEquals", minorThanEquals);
		}
		final AliasInfo exprAlias = getAlias();
		process(minorThanEquals);
		stack.push(processAlias(
				new E_LessThanOrEqual(stack.pop(), stack.pop()), exprAlias));
	}

	@Override
	public void visit(final Multiplication multiplication) {
		if (LOG.isDebugEnabled()) {
			logVisit("Multiplication", multiplication);
		}
		final AliasInfo exprAlias = getAlias();
		process(multiplication);
		stack.push(processAlias(new E_Multiply(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final NotEqualsTo notEqualsTo) {
		if (LOG.isDebugEnabled()) {
			logVisit("Not Equals", notEqualsTo);
		}
		final AliasInfo exprAlias = getAlias();
		process(notEqualsTo);
		stack.push(processAlias(new E_NotEquals(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final NullValue nullValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("Null Value", nullValue);
		}
		throw new UnsupportedOperationException(
				"Figure out how to process NULL");
	}

	@Override
	public void visit(final OrExpression orExpression) {
		if (LOG.isDebugEnabled()) {
			logVisit("Or Expression", orExpression);
		}
		final AliasInfo exprAlias = getAlias();
		process(orExpression);
		stack.push(processAlias(new E_LogicalOr(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final Parenthesis parenthesis) {
		if (LOG.isDebugEnabled()) {
			logVisit("Parenthesis", parenthesis);
		}
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(final StringValue stringValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("String Value", stringValue);
		}
		stack.push(processAlias(new NodeValueString(stringValue.getValue()),
				getAlias()));
	}

	@Override
	public void visit(final SubSelect subSelect) {
		throw new UnsupportedOperationException("SUB SELECT is not supported");
	}

	@Override
	public void visit(final Subtraction subtraction) {
		if (LOG.isDebugEnabled()) {
			logVisit("Subtraction", subtraction);
		}
		final AliasInfo exprAlias = getAlias();
		process(subtraction);
		stack.push(processAlias(new E_Subtract(stack.pop(), stack.pop()),
				exprAlias));
	}

	@Override
	public void visit(final TimestampValue timestampValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("Timestamp", timestampValue);
		}
		final String parts[] = timestampValue.getValue().toString().split(" ");
		final String val = String.format("%sT%s", parts[0], parts[1]);
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDdateTime);
		stack.push(processAlias(new NodeValueDT(val, n), getAlias()));
	}

	@Override
	public void visit(final TimeValue timeValue) {
		if (LOG.isDebugEnabled()) {
			logVisit("TimeValue", timeValue);
		}
		final String val = timeValue.getValue().toString();
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDtime);
		stack.push(processAlias(new NodeValueDT(val, n), getAlias()));
	}

	@Override
	public void visit(final WhenClause whenClause) {
		throw new UnsupportedOperationException("WHEN is not supported");
	}

	/**
	 * A class that extends ExprVar and contains a QueryColumnInfo.
	 *
	 */
	public static class ExprColumn extends ExprVar {
		private final QueryColumnInfo columnInfo;

		public ExprColumn(final QueryColumnInfo columnInfo) {
			super(columnInfo.getGUIDVar());
			this.columnInfo = columnInfo;
		}

		/**
		 * Get the enclosed column Info.
		 *
		 * @return the QueryColumnInfo
		 */
		public QueryColumnInfo getColumnInfo() {
			return columnInfo;
		}

	}

	public class AliasInfo {
		private String alias;
		private boolean aliasRequired;
		private boolean used;

		public AliasInfo(final String alias, final boolean aliasRequired) {
			this.alias = alias;
			this.aliasRequired = aliasRequired;
			this.used = false;
		}

		public String getAlias() {
			if (StringUtils.isBlank(alias)) {
				alias = makeAlias();
			}
			return alias;
		}

		public boolean isAliasRequired() {
			return aliasRequired;
		}

		private String makeAlias() {
			return String.format("var%s", builder.getAliasCount());
		}

		public boolean isUsed() {
			return used;
		}

		public void setUsed(final boolean used) {
			this.used = used;
		}
	}

}