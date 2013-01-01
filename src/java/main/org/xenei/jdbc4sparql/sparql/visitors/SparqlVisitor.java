package org.xenei.jdbc4sparql.sparql.visitors;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;

import java.net.URI;
import java.sql.ResultSet;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
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
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class SparqlVisitor implements StatementVisitor
{
	private SparqlQueryBuilder sparqlQueryBuilder;
	
	public SparqlVisitor(Catalog catalog)
	{
		sparqlQueryBuilder = new SparqlQueryBuilder( catalog );
	}
	
	public SparqlQueryBuilder getBuilder()
	{
		return sparqlQueryBuilder;
	}
	@Override
	public void visit( Select select )
	{
		SparqlSelectVisitor v = new SparqlSelectVisitor( sparqlQueryBuilder );
		select.getSelectBody().accept(v);
	}

	@Override
	public void visit( Delete delete )
	{
		throw new UnsupportedOperationException( "DELETE" ); 
	}

	@Override
	public void visit( Update update )
	{
		throw new UnsupportedOperationException( "UPDATE" ); 
	}

	@Override
	public void visit( Insert insert )
	{
		throw new UnsupportedOperationException( "INSERT" ); 
	}

	@Override
	public void visit( Replace replace )
	{
		throw new UnsupportedOperationException( "REPLACE" ); 
	}

	@Override
	public void visit( Drop drop )
	{
		throw new UnsupportedOperationException( "DROP" ); 
	}

	@Override
	public void visit( Truncate truncate )
	{
		throw new UnsupportedOperationException( "TRUNCATE" ); 
	}

	@Override
	public void visit( CreateTable createTable )
	{
		throw new UnsupportedOperationException( "CREATE TABLE" ); 
	}
	
	ResultSet getResultSet( URI uri)
	{
		
		new SparqlView( uri, sparqlQueryBuilder ).getResultSet();
	}

}
