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

import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.SchemaImpl;
import org.xenei.jdbc4sparql.impl.rdf.RdfSchema;
import org.xenei.jdbc4sparql.impl.rdf.SchemaBuilder;

public class SparqlSchema extends RdfSchema
{
	public static class Builder extends SchemaBuilder {
		
	}
	
	
	@Override
	public void addTables( final Table table )
	{
		super.addTables(verifySparqlTableDef(table));
	}


	private SparqlTable verifySparqlTableDef( final Table table )
	{
		TableDef tableDef = table.getTableDef();
		if (tableDef == null)
		{
			throw new IllegalArgumentException("table def may not be a null");
		}
		if (!(tableDef instanceof SparqlTableDef))
		{
			throw new IllegalStateException(table.getName()
					+ " is not a SPARQL table definition.");
		}
		return (SparqlTable) table;
	}
}
