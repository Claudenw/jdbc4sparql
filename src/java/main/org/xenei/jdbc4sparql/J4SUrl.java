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

public class J4SUrl
{
	/**
	 * URLs of the form
	 * jdbc:J4S:<configlocation>
	 * jdbc:J4S?catalog=cat:<configlocation>
	 * jdbc:J4S?catalog=cat&builder=schema_builder:<sparqlendpint>
	 */
	public static final String SUB_PROTOCOL = "J4S";
	public static final String CATALOG = "catalog";
	public static final String BUILDER = "builder";

	private URI endpoint;
	private String catalog;
	private String builder;

	public J4SUrl( final String urlStr )
	{
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

	private String extractArg( final String urlStr, int startPos )
	{
		if (urlStr.charAt(startPos) != '=')
		{
			throw new IllegalArgumentException(
					"Not a valid J4S JDBC URL -- argument must be followd by an equal sign '='");
		}
		startPos++;
		int endPos = Integer.MAX_VALUE;

		int endPosCk = urlStr.indexOf(':', startPos);
		if (endPosCk != -1)
		{
			endPos = endPosCk;
		}

		endPosCk = urlStr.indexOf('&', startPos);
		if (endPosCk != -1)
		{
			endPos = Math.min(endPosCk, endPos);
		}
		if (endPos == Integer.MAX_VALUE)
		{
			throw new IllegalArgumentException(
					"Not a valid J4S JDBC URL -- argument value must be followd by ':' or '&'");
		}
		return urlStr.substring(startPos, endPos);
	}

	public String getBuilder()
	{
		return builder;
	}

	public String getCatalog()
	{
		return catalog;
	}

	public URI getEndpoint()
	{
		return endpoint;
	}

	// parse the ?catalog=<x>&schema=<y>: as well as the ?catalog=<x>: versions
	private void parseJ4SArgs( final String urlStr, final int startPos )
	{
		int pos = startPos;
		if (!doComp(urlStr, pos, J4SUrl.CATALOG))
		{
			throw new IllegalArgumentException("Not a valid J4S JDBC URL -- '"
					+ J4SUrl.CATALOG + "' must be first argument");
		}
		pos += J4SUrl.CATALOG.length();
		this.catalog = extractArg(urlStr, pos);
		pos += 1 + this.catalog.length();
		if (urlStr.charAt(pos) == '&')
		{
			pos++;
			if (!doComp(urlStr, pos, J4SUrl.BUILDER))
			{
				throw new IllegalArgumentException(
						"Not a valid J4S JDBC URL -- '" + J4SUrl.BUILDER
								+ "' is the only allowable second argument");
			}
			pos += J4SUrl.BUILDER.length();
			this.builder = extractArg(urlStr, pos);
			pos += 1 + this.builder.length();
		}
		if (!(urlStr.charAt(pos) == ':'))
		{
			throw new IllegalArgumentException(
					"Not a valid J4S JDBC URL -- arguments must be followed by a colon ':'");
		}
		parseJ4SEndpoint(urlStr, pos + 1);
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
