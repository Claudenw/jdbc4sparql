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
package org.xenei.jdbc4sparql.mock;

import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;

public class MockSchema extends SparqlSchema
{
	public static final String LOCAL_NAME = "MockSchema";

	public MockSchema()
	{
		this(new MockCatalog());
	}

	public MockSchema( final SparqlCatalog catalog )
	{
		this(catalog, MockSchema.LOCAL_NAME);
	}

	public MockSchema( final SparqlCatalog catalog, final String schema )
	{
		super(catalog, MockCatalog.NS, schema);
	}

	@Override
	public Table newTable( final String name )
	{
		final TableDef tableDef = getTableDef(name);
		if (tableDef == null)
		{
			throw new IllegalArgumentException(name
					+ " is not a table in this schema");
		}
		return new MockTable(this, (MockTableDef) tableDef);
	}
}