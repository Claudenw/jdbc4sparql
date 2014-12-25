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
import org.xenei.jdbc4sparql.iface.name.TableName;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.items.QueryColumnInfo;
import org.xenei.jdbc4sparql.sparql.items.QueryTableInfo;
import org.xenei.jdbc4sparql.sparql.parser.jsqlparser.functions.AbstractFunctionHandler.FuncInfo;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Divide;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_Multiply;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.E_Regex;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDT;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

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

	private String alias;

	private static Logger LOG = LoggerFactory
			.getLogger(SparqlExprVisitor.class);

	/**
	 * Constructor
	 *
	 * @param builder
	 *            The SparqlQueryBuilder to use.
	 */
	public SparqlExprVisitor(final SparqlQueryBuilder builder,
			final boolean optionalColumns, final String alias) {
		this.builder = builder;
		this.optionalColumns = optionalColumns;
		this.alias = alias;
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
			final boolean optionalColumns) {
		this(builder, optionalColumns, null);
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

	public String getAlias() {
		return alias;
	}

	/**
	 *
	 * @return True if the stack is empty (no result).
	 */
	public boolean isEmpty() {
		return stack.isEmpty();
	}

	// process a binary expression.
	private void process(final BinaryExpression biExpr) {
		// put on in reverse order so they can be popped back off in the proper
		// order.
		biExpr.getRightExpression().accept(this);
		biExpr.getLeftExpression().accept(this);
	}

	@Override
	public void visit(final Addition addition) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Addition: {}", addition);
		}
		process(addition);
		stack.push(new E_Add(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final AllComparisonExpression allComparisonExpression) {
		throw new UnsupportedOperationException("ALL is not supported");
	}

	@Override
	public void visit(final AndExpression andExpression) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit And: {}", andExpression);
		}
		process(andExpression);
		stack.push(new E_LogicalAnd(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final AnyComparisonExpression anyComparisonExpression) {
		throw new UnsupportedOperationException("ANY is not supported");
	}

	@Override
	public void visit(final Between between) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Between: {}", between);
		}
		between.getBetweenExpressionEnd().accept(this);
		between.getBetweenExpressionStart().accept(this);
		between.getLeftExpression().accept(this);
		// rewrite as x <= a >= y
		final Expr a = stack.pop();
		final Expr left = new E_LessThanOrEqual(stack.pop(), a);
		final Expr right = new E_GreaterThanOrEqual(a, stack.pop());
		stack.push(new E_LogicalAnd(left, right));
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
			final String aliasLog = alias == null ? "" : String.format(
					" as %s", alias);
			SparqlExprVisitor.LOG.debug("visit Column: {}{}", tableColumn,
					aliasLog);
		}
		final String schema = StringUtils.defaultString(tableColumn.getTable()
				.getSchemaName(), builder.getDefaultSchemaName());
		final String table = StringUtils.defaultString(tableColumn.getTable()
				.getName(), builder.getDefaultTableName());
		final ColumnName cName = new ColumnName(builder.getCatalogName(),
				schema, table, tableColumn.getColumnName());
		final TableName tName = cName.getTableName();
		final QueryTableInfo tableInfo = builder.getTable(tName);
		QueryColumnInfo columnInfo = tableInfo.getColumn(cName);
		if (alias != null) {
			final ColumnName aliasName = ColumnName.getNameInstance(cName,
					alias);
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
			SparqlExprVisitor.LOG.debug("visit DateValue: {}", dateValue);
		}
		final String val = dateValue.getValue().toString();
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDdate);
		stack.push(new NodeValueDT(val, n));
	}

	@Override
	public void visit(final Division division) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Division: {}", division);
		}
		process(division);
		stack.push(new E_Divide(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final DoubleValue doubleValue) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit DoubleValue: {}", doubleValue);
		}
		stack.push(new NodeValueDouble(doubleValue.getValue()));
	}

	@Override
	public void visit(final EqualsTo equalsTo) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit EqualsTo: {}", equalsTo);
		}
		process(equalsTo);
		stack.push(new E_Equals(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final ExistsExpression existsExpression) {
		throw new UnsupportedOperationException("EXISTS is not supported");
	}

	@Override
	public void visit(final Function function) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Function: {}", function);
		}
		final StandardFunctionHandler sfh = new StandardFunctionHandler(builder);
		try {
			if (alias == null) {
				alias = function.toString().replaceAll("[^A-Za-z0-9]", "_");
			}

			final FuncInfo funcInfo = sfh.handle(function, alias);
			stack.push(funcInfo);
		} catch (final SQLException e) {
			throw new UnsupportedOperationException(String.format(
					"function %s is not supported (%s)", function.getName(),
					e.getMessage()));
		}
	}

	@Override
	public void visit(final GreaterThan greaterThan) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit GreaterThan: {}", greaterThan);
		}
		process(greaterThan);
		stack.push(new E_GreaterThan(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final GreaterThanEquals greaterThanEquals) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit GreaterThanEquals: {}",
					greaterThanEquals);
		}
		process(greaterThanEquals);
		stack.push(new E_GreaterThanOrEqual(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final InExpression inExpression) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit InExpression: {}", inExpression);
		}

		final SparqlItemsListVisitor listVisitor = new SparqlItemsListVisitor(
				builder);
		inExpression.getItemsList().accept(listVisitor);
		inExpression.getLeftExpression().accept(this);
		stack.push(new E_OneOf(stack.pop(), listVisitor.getResult()));
	}

	@Override
	public void visit(final InverseExpression inverseExpression) {
		throw new UnsupportedOperationException(
				"inverse expressions are not supported");
	}

	@Override
	public void visit(final IsNullExpression isNullExpression) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit isNull: {}", isNullExpression);
		}

		isNullExpression.getLeftExpression().accept(this);
		ExprFunction func = new E_Bound(stack.pop());

		if (isNullExpression.isNot()) {
			func = new E_LogicalNot(func);

		}
		stack.push(func);
	}

	@Override
	public void visit(final JdbcParameter jdbcParameter) {
		throw new UnsupportedOperationException(
				"JDBC Parameters are not supported");
	}

	@Override
	public void visit(final LikeExpression likeExpression) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit LikeExpression: {}",
					likeExpression);
		}
		process(likeExpression);
		final Expr left = stack.pop();
		final Expr right = stack.pop();
		if (right instanceof NodeValueString) {
			final RegexNodeValue rnv = RegexNodeValue
					.create(((NodeValueString) right).getString());
			if (rnv.isWildcard()) {
				stack.push(new E_Regex(left, rnv, new NodeValueString("")));
			}
			else {
				stack.push(new E_Equals(left, rnv));
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
			SparqlExprVisitor.LOG.debug("visit Long: {}", longValue);
		}
		stack.push(new NodeValueInteger(longValue.getValue()));
	}

	@Override
	public void visit(final Matches matches) {
		throw new UnsupportedOperationException("MATCHES is not supported");
	}

	@Override
	public void visit(final MinorThan minorThan) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit MinorThan: {}", minorThan);
		}
		process(minorThan);
		stack.push(new E_LessThan(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final MinorThanEquals minorThanEquals) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit MinorThanEquals: {}",
					minorThanEquals);
		}
		process(minorThanEquals);
		stack.push(new E_LessThanOrEqual(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final Multiplication multiplication) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Multiplication: {}",
					multiplication);
		}
		process(multiplication);
		stack.push(new E_Multiply(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final NotEqualsTo notEqualsTo) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit not equals: {}", notEqualsTo);
		}
		process(notEqualsTo);
		stack.push(new E_NotEquals(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final NullValue nullValue) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit null value: {}", nullValue);
		}
		throw new UnsupportedOperationException(
				"Figure out how to process NULL");
	}

	@Override
	public void visit(final OrExpression orExpression) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit orExpression: {}", orExpression);
		}
		process(orExpression);
		stack.push(new E_LogicalOr(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final Parenthesis parenthesis) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Parenthesis: {}", parenthesis);
		}
		parenthesis.getExpression().accept(this);
	}

	@Override
	public void visit(final StringValue stringValue) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit String Value: {}", stringValue);
		}
		stack.push(new NodeValueString(stringValue.getValue()));
	}

	@Override
	public void visit(final SubSelect subSelect) {
		throw new UnsupportedOperationException("SUB SELECT is not supported");
	}

	@Override
	public void visit(final Subtraction subtraction) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Subtraction: {}", subtraction);
		}
		process(subtraction);
		stack.push(new E_Subtract(stack.pop(), stack.pop()));
	}

	@Override
	public void visit(final TimestampValue timestampValue) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit Timestamp: {}", timestampValue);
		}
		final String parts[] = timestampValue.getValue().toString().split(" ");
		final String val = String.format("%sT%s", parts[0], parts[1]);
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDdateTime);
		stack.push(new NodeValueDT(val, n));
	}

	@Override
	public void visit(final TimeValue timeValue) {
		if (LOG.isDebugEnabled()) {
			SparqlExprVisitor.LOG.debug("visit TimeValue: {}", timeValue);
		}
		final String val = timeValue.getValue().toString();
		final Node n = NodeFactory.createLiteral(val, XSDDatatype.XSDtime);
		stack.push(new NodeValueDT(val, n));
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
			super(columnInfo.getVar());
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

}