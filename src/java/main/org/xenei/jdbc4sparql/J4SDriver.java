/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class J4SDriver implements Driver
{

	public static void main( final String[] args )
	{

		final J4SDriver driver = new J4SDriver();
		System.out.println(String.format("%s Version %s.%s", driver.getName(),
				driver.getMajorVersion(), driver.getMinorVersion()));

		System.out.println("Registered Schema Builders:");
		System.out.print("(Default) ");
		final List<Class<? extends SchemaBuilder>> builderList = SchemaBuilder.Util
				.getBuilders();
		for (final Class<? extends SchemaBuilder> c : builderList)
		{
			System.out.println(SchemaBuilder.Util.getName(c) + ": "
					+ SchemaBuilder.Util.getDescription(c));
		}
		System.out.println();
		System.out.println("Registered SPARQL parsers:");
		System.out.println("(Default)");
		final List<Class<? extends SparqlParser>> parserList = SparqlParser.Util
				.getParsers();
		for (final Class<? extends SparqlParser> c : parserList)
		{
			System.out.println(SparqlParser.Util.getName(c) + ": "
					+ SparqlParser.Util.getDescription(c));
		}
	}

	public J4SDriver()
	{
	}

	@Override
	public boolean acceptsURL( final String url ) throws SQLException
	{
		try
		{
			new J4SUrl(url);
			return true;
		}
		catch (final IllegalArgumentException e)
		{
			return false;
		}
	}

	@Override
	public Connection connect( final String urlStr, final Properties props )
			throws SQLException
	{
		J4SUrl url = null;

		try
		{
			url = new J4SUrl(urlStr);
		}
		catch (final IllegalArgumentException e)
		{
			return null; // return null if the URL is not valid for us.
		}
		try
		{
			return new J4SConnection(this, url, props);
		}
		catch (final MalformedURLException e)
		{
			throw new SQLException(e);
		}
	}

	@Override
	public int getMajorVersion()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMinorVersion()
	{
		// TODO Auto-generated method stub
		return 1;
	}

	public String getName()
	{
		return "Jdbc 4 Sparql Driver";
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo( final String url,
			final Properties info ) throws SQLException
	{
		try
		{
			new J4SUrl(url);
			return new DriverPropertyInfo[0];
		}
		catch (final IllegalArgumentException e)
		{
			throw new SQLException(e);
		}
	}

	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

}
