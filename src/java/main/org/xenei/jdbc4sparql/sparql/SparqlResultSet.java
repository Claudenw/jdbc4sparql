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

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.RDFNode;

import java.sql.SQLException;

import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.ListResultSet;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;

public class SparqlResultSet extends ListResultSet
{

	public SparqlResultSet( final RdfTable table ) throws SQLException
	{
		super(table.getCatalog().executeLocalQuery(table.getQuery()), table);
	}

	@Override
	protected RdfTable getTable()
	{
		return (RdfTable) super.getTable();
	}

	@Override
	protected Object readObject( final int columnOrdinal ) throws SQLException
	{
		checkPosition();
		checkColumn(columnOrdinal);
		final QuerySolution soln = (QuerySolution) getRowObject();
		final String colName = getTable().getSolutionName(columnOrdinal - 1);
		final RDFNode node = soln.get(colName);

		if (node == null)
		{
			return null;
		}
		if (node.isLiteral())
		{
			return TypeConverter.getJavaValue(node.asLiteral());
		}
		return node.toString();
	}
}
