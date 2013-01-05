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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.openjena.riot.Lang;
import org.xenei.jdbc4sparql.sparql.builders.SchemaBuilder;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class J4SUrl
{
	/**
	 * URLs of the form
	 * jdbc:J4S:<configlocation>
	 * jdbc:J4S?catalog=cat:<configlocation>
	 * jdbc:J4S?catalog=cat&builder=builderclass:<sparqlendpint>
	 */
	public static final String SUB_PROTOCOL = "J4S";
	public static final String CATALOG = "catalog";
	public static final String BUILDER = "builder";
	public static final String PARSER = "parser";
	public static final String TYPE = "type";
	public static final String TYPE_SPARQL="sparql";
	public static final String TYPE_CONFIG="config";
	public static final String[] ARGS = { CATALOG, TYPE, BUILDER, PARSER }; 
	
	private URI endpoint;
	private SparqlParser parser;
	private SchemaBuilder builder;
	private Properties properties;

	/**
	 * Parses are URL of the form 
	 * jdbc:j4s[?ARG=Val[&ARG=VAL...]]:[URI]
	 * @param urlStr
	 */
	public J4SUrl( final String urlStr ) 
	{
		this.properties = new Properties();
		final String jdbc = "jdbc:";
		int pos = 0;
		if (!doComp(urlStr, pos, jdbc))
		{
			throw new IllegalArgumentException("Not a JDBC URL");
		}
		pos += jdbc.length();
		if (!doComp(urlStr, pos, J4SUrl.SUB_PROTOCOL))
		{
			throw new IllegalArgumentException("Not a J4S JDBC URL");
		}
		pos += J4SUrl.SUB_PROTOCOL.length();
		if (urlStr.charAt(pos) == '?')
		{
			parseJ4SArgs(urlStr, pos + 1);
		}
		else if (urlStr.charAt(pos) == ':')
		{
			parseJ4SEndpoint(urlStr, pos + 1);
		}
		else
		{
			throw new IllegalArgumentException(
					"Not a valid J4S JDBC URL -- missing endpoint");
		}
	}

	private boolean doComp( final String target, final int pos,
			final String comp )
	{
		target.substring(pos, pos + comp.length());
		return ((pos + comp.length()) < target.length())
				&& target.substring(pos, pos + comp.length()).equalsIgnoreCase(
						comp);
	}

	public Properties getProperties()
	{
		return properties;
	}
	
	public String getCatalog()
	{
		return properties.getProperty(CATALOG, "");
	}
	
	public SchemaBuilder getBuilder()
	{
		return builder;
	}
	
	public SparqlParser getParser()
	{
		return parser;
	}

	public String getType()
	{
		return properties.getProperty(TYPE, TYPE_CONFIG );
	}

	public URI getEndpoint()
	{
		return endpoint;
	}

	// parse the ?catalog=<x>&schema=<y>: as well as the ?catalog=<x>: versions
	/**
	 * Parse an argument out of the URL string section.
	 * 
	 * Should be of the form x=y[:|&]
	 * @param urlStr
	 * @param startPos
	 */
	private void parseJ4SArgs( final String urlStr, final int startPos )
	{
		
		int pos = startPos;
		// (arg)=(val)(:|&)
		Pattern pattern =  Pattern.compile( "(([a-zA-Z]+)\\=([^:\\&]+)([:|\\&])).+" );
		Matcher matcher = pattern.matcher( urlStr.substring(startPos) );
		
		while (matcher.matches())
		{	
			String arg = matcher.group(2);
			boolean found = false;
			for (String validArg : ARGS)
			{
				found |= validArg.equalsIgnoreCase(arg);
			}
			if (!found)
			{
				throw new IllegalArgumentException(
						"Not a valid J4S JDBC URL -- '" + arg
						+ "' is not a recognized argument");
			}
			properties.put( arg, matcher.group(3));
			pos += matcher.group(1).length();
			matcher = pattern.matcher( urlStr.substring(pos) );
		}
		
		// check for valid type value and make sure it is upper case.
		// valid type is a Jena Lang.
		if (properties.containsKey( TYPE ))
		{
			String type = properties.getProperty(TYPE);
			if (type.equalsIgnoreCase( TYPE_SPARQL))
			{
				properties.setProperty(TYPE, TYPE_SPARQL);
			}
			else
			{
				boolean found = false;
				for (Lang l : Lang.values())
				{
					if (l.name().equalsIgnoreCase(type))
					{
						found = true;
						properties.setProperty(TYPE, l.getName());
					}
					else if (l.getName().equalsIgnoreCase(type))
					{
						found = true;
						properties.setProperty(TYPE, l.getName());
					}
				}
				if (!found)
				{
					throw new IllegalArgumentException(
							"Not a valid J4S JDBC URL -- '" + type
							+ "' is not a recognized type value");
				}
			}
		}
		if (properties.containsKey( PARSER ))
		{
			// verify we can load the parser
			parser = SparqlParser.Util.getParser( properties.getProperty( PARSER ));
		}
		if (properties.containsKey( BUILDER ))
		{
			// verify we can load the builder
			builder = SchemaBuilder.Util.getBuilder( properties.getProperty( BUILDER ));
		}
		parseJ4SEndpoint(urlStr, pos);
	}

	private void parseJ4SEndpoint( final String urlStr, final int pos )
	{
		try
		{
			this.endpoint = new URI(urlStr.substring(pos));
		}
		catch (final URISyntaxException e)
		{
			throw new IllegalArgumentException(
					"Not a valid J4S JDBC URL -- endpoint is not a valid URI : "
							+ e.toString());
		}
	}

}
