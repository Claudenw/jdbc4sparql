package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
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
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDT;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.vocabulary.RDF;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;

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

class SparqlExprVisitor implements ExpressionVisitor 
	{
		private SparqlQueryBuilder builder;
		private Stack<Expr> stack;
	
		SparqlExprVisitor(SparqlQueryBuilder builder)
		{
			this.builder = builder;
			stack = new Stack<Expr>();	
		}

		public Expr getResult()
		{
			return stack.pop();
		}
		
		public boolean isEmpty()
		{
			return stack.isEmpty();
		}

		@Override
		public void visit( NullValue nullValue )
		{
			throw new UnsupportedOperationException( "Figure out how to process NULL");
		}

		@Override
		public void visit( Function function )
		{
			throw new UnsupportedOperationException( "functions are not supported");
		}

		@Override
		public void visit( InverseExpression inverseExpression )
		{
			throw new UnsupportedOperationException( "inverse expressions are not supported");		}

		@Override
		public void visit( JdbcParameter jdbcParameter )
		{
			throw new UnsupportedOperationException( "JDBC Parameters are not supported");
		}

		
		@Override
		public void visit( DoubleValue doubleValue )
		{
			stack.push( new NodeValueDouble( doubleValue.getValue() ) );
		}

		@Override
		public void visit( LongValue longValue )
		{
			stack.push( new NodeValueInteger( longValue.getValue() ));
		}

		@Override
		public void visit( DateValue dateValue )
		{
			String val = dateValue.getValue().toString();
			Node n = Node.createLiteral(val, XSDDatatype.XSDdate);
			stack.push( new NodeValueDT( val, n) );
		}

		@Override
		public void visit( TimeValue timeValue )
		{
			String val = timeValue.getValue().toString();
			Node n = Node.createLiteral(val, XSDDatatype.XSDtime);
			stack.push( new NodeValueDT( val, n) );
		}

		@Override
		public void visit( TimestampValue timestampValue )
		{
			String val = timestampValue.getValue().toString();
			Node n = Node.createLiteral(val, XSDDatatype.XSDdateTime);
			stack.push( new NodeValueDT( val, n) );
		}
		@Override
		public void visit( Parenthesis parenthesis )
		{
			throw new UnsupportedOperationException( "Parenthesis are not supported");
//			buffer.append("(");
//	        parenthesis.getExpression().accept(this);
//	        buffer.append(")");
		}

		@Override
		public void visit( StringValue stringValue )
		{
			stack.push( new NodeValueString( stringValue.getValue() ) );
		}

		private void process( BinaryExpression biExpr )
		{
			// put on in reverse order so they can be popped back off in the proper order.
			biExpr.getRightExpression().accept( this );
			biExpr.getLeftExpression().accept( this );
		}
		@Override
		public void visit( Addition addition )
		{
			process( addition );
			stack.push( new E_Add(stack.pop(), stack.pop()));
		}

		@Override
		public void visit( Division division )
		{
			process( division );
			stack.push( new E_Divide(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( Multiplication multiplication )
		{
			process( multiplication);
			stack.push( new E_Multiply(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( Subtraction subtraction )
		{
			process( subtraction );
			stack.push( new E_Subtract(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( AndExpression andExpression )
		{
			process( andExpression );
			stack.push( new E_LogicalAnd(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( OrExpression orExpression )
		{
			process( orExpression );
			stack.push( new E_LogicalOr(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( Between between )
		{
			between.getBetweenExpressionEnd().accept( this );
			between.getBetweenExpressionStart().accept( this );
			between.getLeftExpression().accept( this );
			// rewrite as x <= a >= y
			Expr a = stack.pop();
			Expr left = new E_LessThanOrEqual(stack.pop(), a);
			Expr right = new E_GreaterThanOrEqual(a, stack.pop());
			stack.push( new E_LogicalAnd(left, right));
		}

		@Override
		public void visit( EqualsTo equalsTo )
		{
			process( equalsTo );
			Expr left = stack.pop();
			Expr right = stack.pop();
			// if 2 vars then add to the SPARQL where to select and as a Filter
			if ((left instanceof NodeValueNode) && (right instanceof NodeValueNode))
			{
				NodeValueNode nLeft = (NodeValueNode)left;
				NodeValueNode nRight = (NodeValueNode)right;
				if (nLeft.getNode().isVariable() && nRight.getNode().isVariable())
				{
					builder.addEquals( nLeft.getNode(), nRight.getNode() );
				}
			}
			stack.push( new E_Equals(left,right));
		}

		@Override
		public void visit( GreaterThan greaterThan )
		{
			process( greaterThan );
			stack.push( new E_GreaterThan(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( GreaterThanEquals greaterThanEquals )
		{
			process( greaterThanEquals );
			stack.push( new E_GreaterThanOrEqual(stack.pop(),stack.pop()));
		}

		@Override
		public void visit( InExpression inExpression )
		{
			throw new UnsupportedOperationException( "IN is not supported");
		}

		@Override
		public void visit( IsNullExpression isNullExpression )
		{
			isNullExpression.getLeftExpression().accept( this );
			stack.push( new E_Equals( stack.pop(), null ));
		}

		@Override
		public void visit( LikeExpression likeExpression )
		{
			// convert this to a regex function.
			throw new UnsupportedOperationException( "LIKE is not supported");
		}

		@Override
		public void visit( MinorThan minorThan )
		{
			process( minorThan );
			stack.push( new E_LessThan( stack.pop(),stack.pop() ));
		}

		@Override
		public void visit( MinorThanEquals minorThanEquals )
		{
			process( minorThanEquals );
			stack.push( new E_LessThanOrEqual( stack.pop(),stack.pop() ));
		}

		@Override
		public void visit( NotEqualsTo notEqualsTo )
		{
			process( notEqualsTo );
			stack.push( new E_NotEquals( stack.pop(),stack.pop() ));
		}

		
		
		@Override
		public void visit( Column tableColumn )
		{
			try {
			Node columnVar = builder.addColumn( tableColumn.getTable().getSchemaName(),
					tableColumn.getTable().getName(), tableColumn.getColumnName());
			/**
			 * Add column to expression
			 */
			stack.push( new NodeValueNode( columnVar ));
			}
			catch (SQLException e)
			{
				throw new RuntimeException( e );
			}
		}

		@Override
		public void visit( SubSelect subSelect )
		{
			throw new UnsupportedOperationException( "SUB SELECT is not supported");
		}

		@Override
		public void visit( CaseExpression caseExpression )
		{
			throw new UnsupportedOperationException( "CASE is not supported");
		}

		@Override
		public void visit( WhenClause whenClause )
		{
			throw new UnsupportedOperationException( "WHEN is not supported");
		}

		@Override
		public void visit( ExistsExpression existsExpression )
		{
			throw new UnsupportedOperationException( "EXISTS is not supported");
		}

		@Override
		public void visit( AllComparisonExpression allComparisonExpression )
		{
			throw new UnsupportedOperationException( "ALL is not supported");
		}

		@Override
		public void visit( AnyComparisonExpression anyComparisonExpression )
		{
			throw new UnsupportedOperationException( "ANY is not supported");
		}

		@Override
		public void visit( Concat concat )
		{
			throw new UnsupportedOperationException( "CONCAT is not supported");
		}

		@Override
		public void visit( Matches matches )
		{
			throw new UnsupportedOperationException( "MATCHES is not supported");
		}

		@Override
		public void visit( BitwiseAnd bitwiseAnd )
		{
			throw new UnsupportedOperationException( "'&' is not supported");
		}

		@Override
		public void visit( BitwiseOr bitwiseOr )
		{
			throw new UnsupportedOperationException( "'|' is not supported");
		}

		@Override
		public void visit( BitwiseXor bitwiseXor )
		{
			throw new UnsupportedOperationException( "'^' is not supported");
		}
	
	}