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

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.E_Add;
import com.hp.hpl.jena.sparql.expr.E_Divide;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_Multiply;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_Subtract;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDT;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

import java.sql.SQLException;
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

import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

/**
 * An expression visitor. Merges SQL expressions into the SparqlQueryBuilder.
 */
class SparqlExprVisitor implements ExpressionVisitor
{
	// the query builder
	private final SparqlQueryBuilder builder;
	// A stack of expression elements.
	private final Stack<Expr> stack;

	/**
	 * Constructor
	 * 
	 * @param builder
	 *            The SparqlQueryBuilder to use.
	 */
	SparqlExprVisitor( final SparqlQueryBuilder builder )
	{
		this.builder = builder;
		stack = new Stack<Expr>();
	}

	/**
	 * Get the final result of the process.
	 * 
	 * @return
	 */
	public Expr getResult()
	{
		return stack.pop();
	}

	/**
	 * 
	 * @return True if the stack is empty (no result).
	 */
	public boolean isEmpty()
	{
		return stack.isEmpty();
	}

	// process a binary expression.
	private void process( final BinaryExpression biExpr )
	{
		// put on in reverse order so they can be popped back off in the proper
		// order.
		biExpr.getRightExpression().accept(this);
		biExpr.getLeftExpression().accept(this);
	}

	@Override
	public void visit( final Addition addition )
	{
		process(addition);
		stack.push(new E_Add(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final AllComparisonExpression allComparisonExpression )
	{
		throw new UnsupportedOperationException("ALL is not supported");
	}

	@Override
	public void visit( final AndExpression andExpression )
	{
		process(andExpression);
		stack.push(new E_LogicalAnd(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final AnyComparisonExpression anyComparisonExpression )
	{
		throw new UnsupportedOperationException("ANY is not supported");
	}

	@Override
	public void visit( final Between between )
	{
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
	public void visit( final BitwiseAnd bitwiseAnd )
	{
		throw new UnsupportedOperationException("'&' is not supported");
	}

	@Override
	public void visit( final BitwiseOr bitwiseOr )
	{
		throw new UnsupportedOperationException("'|' is not supported");
	}

	@Override
	public void visit( final BitwiseXor bitwiseXor )
	{
		throw new UnsupportedOperationException("'^' is not supported");
	}

	@Override
	public void visit( final CaseExpression caseExpression )
	{
		throw new UnsupportedOperationException("CASE is not supported");
	}

	@Override
	public void visit( final Column tableColumn )
	{
		try
		{
			final Node columnVar = builder.addColumn(tableColumn.getTable()
					.getSchemaName(), tableColumn.getTable().getName(),
					tableColumn.getColumnName());
			stack.push(new ExprVar(columnVar));
		}
		catch (final SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void visit( final Concat concat )
	{
		throw new UnsupportedOperationException("CONCAT is not supported");
	}

	@Override
	public void visit( final DateValue dateValue )
	{
		final String val = dateValue.getValue().toString();
		final Node n = Node.createLiteral(val, XSDDatatype.XSDdate);
		stack.push(new NodeValueDT(val, n));
	}

	@Override
	public void visit( final Division division )
	{
		process(division);
		stack.push(new E_Divide(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final DoubleValue doubleValue )
	{
		stack.push(new NodeValueDouble(doubleValue.getValue()));
	}

	@Override
	public void visit( final EqualsTo equalsTo )
	{
		process(equalsTo);
		stack.push(new E_Equals(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final ExistsExpression existsExpression )
	{
		throw new UnsupportedOperationException("EXISTS is not supported");
	}

	@Override
	public void visit( final Function function )
	{
		throw new UnsupportedOperationException("functions are not supported");
	}

	@Override
	public void visit( final GreaterThan greaterThan )
	{
		process(greaterThan);
		stack.push(new E_GreaterThan(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final GreaterThanEquals greaterThanEquals )
	{
		process(greaterThanEquals);
		stack.push(new E_GreaterThanOrEqual(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final InExpression inExpression )
	{
		throw new UnsupportedOperationException("IN is not supported");
	}

	@Override
	public void visit( final InverseExpression inverseExpression )
	{
		throw new UnsupportedOperationException(
				"inverse expressions are not supported");
	}

	@Override
	public void visit( final IsNullExpression isNullExpression )
	{
		isNullExpression.getLeftExpression().accept(this);
		stack.push(new E_Equals(stack.pop(), null));
	}

	@Override
	public void visit( final JdbcParameter jdbcParameter )
	{
		throw new UnsupportedOperationException(
				"JDBC Parameters are not supported");
	}

	@Override
	public void visit( final LikeExpression likeExpression )
	{
		// TODO convert this to a regex function.
		throw new UnsupportedOperationException("LIKE is not supported");
	}

	@Override
	public void visit( final LongValue longValue )
	{
		stack.push(new NodeValueInteger(longValue.getValue()));
	}

	@Override
	public void visit( final Matches matches )
	{
		throw new UnsupportedOperationException("MATCHES is not supported");
	}

	@Override
	public void visit( final MinorThan minorThan )
	{
		process(minorThan);
		stack.push(new E_LessThan(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final MinorThanEquals minorThanEquals )
	{
		process(minorThanEquals);
		stack.push(new E_LessThanOrEqual(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final Multiplication multiplication )
	{
		process(multiplication);
		stack.push(new E_Multiply(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final NotEqualsTo notEqualsTo )
	{
		process(notEqualsTo);
		stack.push(new E_NotEquals(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final NullValue nullValue )
	{
		throw new UnsupportedOperationException(
				"Figure out how to process NULL");
	}

	@Override
	public void visit( final OrExpression orExpression )
	{
		process(orExpression);
		stack.push(new E_LogicalOr(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final Parenthesis parenthesis )
	{
		throw new UnsupportedOperationException("Parenthesis are not supported");
		// buffer.append("(");
		// parenthesis.getExpression().accept(this);
		// buffer.append(")");
	}

	@Override
	public void visit( final StringValue stringValue )
	{
		stack.push(new NodeValueString(stringValue.getValue()));
	}

	@Override
	public void visit( final SubSelect subSelect )
	{
		throw new UnsupportedOperationException("SUB SELECT is not supported");
	}

	@Override
	public void visit( final Subtraction subtraction )
	{
		process(subtraction);
		stack.push(new E_Subtract(stack.pop(), stack.pop()));
	}

	@Override
	public void visit( final TimestampValue timestampValue )
	{
		final String val = timestampValue.getValue().toString();
		final Node n = Node.createLiteral(val, XSDDatatype.XSDdateTime);
		stack.push(new NodeValueDT(val, n));
	}

	@Override
	public void visit( final TimeValue timeValue )
	{
		final String val = timeValue.getValue().toString();
		final Node n = Node.createLiteral(val, XSDDatatype.XSDtime);
		stack.push(new NodeValueDT(val, n));
	}

	@Override
	public void visit( final WhenClause whenClause )
	{
		throw new UnsupportedOperationException("WHEN is not supported");
	}

}