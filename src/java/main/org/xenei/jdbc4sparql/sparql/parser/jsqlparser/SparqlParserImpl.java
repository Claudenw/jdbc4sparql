package org.xenei.jdbc4sparql.sparql.parser.jsqlparser;

import java.io.StringReader;
import java.sql.SQLException;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlQueryBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlParserImpl implements SparqlParser
{
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
	private final SparqlCatalog catalog;

	public SparqlParserImpl( final SparqlCatalog catalog )
	{
		this.catalog = catalog;
	}

	@Override
	public SparqlQueryBuilder parse( final String sqlQuery )
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

}
