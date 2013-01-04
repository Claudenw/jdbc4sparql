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
	public static final String PARSER_NAME = "JSqlParser";
	public static final String DESCRIPTION = "Parser based on JSqlParser (http://jsqlparser.sourceforge.net/). Under LGPL V2 license";
	private final CCJSqlParserManager parserManager = new CCJSqlParserManager();

	public SparqlParserImpl()
	{
	}

	@Override
	public String nativeSQL( final String sqlQuery ) throws SQLException
	{
		try
		{
			final Statement stmt = parserManager.parse(new StringReader(
					sqlQuery));
			final StringBuffer sb = new StringBuffer();
			final StatementDeParser dp = new StatementDeParser(sb);
			stmt.accept(dp);
			return sb.toString();
		}
		catch (final JSQLParserException e)
		{
			throw new SQLException(e);
		}
	}

	@Override
	public SparqlQueryBuilder parse( final SparqlCatalog catalog,
			final String sqlQuery ) throws SQLException
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
