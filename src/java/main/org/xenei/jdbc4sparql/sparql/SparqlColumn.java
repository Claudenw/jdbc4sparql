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
package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.impl.ColumnImpl;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

public class SparqlColumn extends ColumnImpl
{

	public SparqlColumn( final SparqlTable table,
			final SparqlColumnDef columnDef )
	{
		super(columnDef.getNamespace(), table, columnDef);
	}

	public List<Triple> getQuerySegments( final Node tableVar,
			final Node columnVar )
	{
		final List<Triple> retval = new ArrayList<Triple>();
		final String fqName = "<" + getFQName() + ">";
		for (final String segment : ((SparqlColumnDef) getColumnDef())
				.getQuerySegments())
		{
			if (!segment.trim().startsWith("#"))
			{
				final List<String> parts = SparqlParser.Util
						.parseQuerySegment(String.format(segment, tableVar,
								columnVar, fqName));
				if (parts.size() != 3)
				{
					throw new IllegalStateException(getFQName()
							+ " query segment " + segment
							+ " does not parse into 3 components");
				}
				retval.add(new Triple(
						SparqlParser.Util.parseNode(parts.get(0)),
						SparqlParser.Util.parseNode(parts.get(1)),
						SparqlParser.Util.parseNode(parts.get(2))));
			}
		}
		return retval;
	}

	@Override
	public SparqlTable getTable()
	{
		return (SparqlTable) super.getTable();
	}

	public boolean isOptional()
	{
		return getNullable() != DatabaseMetaData.columnNoNulls;
	}
}
