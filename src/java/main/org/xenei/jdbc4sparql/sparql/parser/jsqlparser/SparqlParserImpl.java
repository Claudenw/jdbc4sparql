package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.io.StringReader;
import java.sql.SQLException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlParserImpl implements SparqlParser
{
	public static final String PARSER_NAME="JSqlParser";
	public static final String DESCRIPTION="Parser based on JSqlParser (http://jsqlparser.sourceforge.net/). Under LGPL V2 license";
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();


	public SparqlParserImpl()
	{
	}

	@Override
	public SparqlQueryBuilder parse( SparqlCatalog catalog, final String sqlQuery )
			throws SQLException
	{
		try
		{
			final Statement stmt = parserManager.parse(new StringReader(
					sqlQuery));
			final SparqlVisitor sv = new SparqlVisitor(catalog);
			stmt.accept(sv);
			return sv.getBuilder();
		}
		catch (final JSQLParserException e)
		{
			throw new SQLException(e);
		}
	}

	@Override
	public String nativeSQL( String sqlQuery ) throws SQLException
	{
		try
		{
			final Statement stmt = parserManager.parse(new StringReader(
					sqlQuery));
			StringBuffer sb = new StringBuffer();
			final StatementDeParser dp = new StatementDeParser( sb );
			stmt.accept(dp);
			return sb.toString();
		}
		catch (final JSQLParserException e)
		{
			throw new SQLException(e);
		}
	}
}
