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
package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Iterator;

import org.xenei.jdbc4sparql.impl.ColumnImpl;

public interface Table extends NamespacedObject, TableDef
{
	public static class ColumnIterator implements Iterator<Column>
	{
		private final Table table;
		private final String namespace;
		private final Iterator<? extends ColumnDef> iter;

		public ColumnIterator( final String namespace, final Table table,
				final Collection<? extends ColumnDef> def )
		{
			this.table = table;
			this.namespace = namespace;
			iter = def.iterator();
		}

		public ColumnIterator( final Table table,
				final Collection<? extends ColumnDef> def )
		{
			this(table.getNamespace(), table, def);
		}

		@Override
		public boolean hasNext()
		{
			return iter.hasNext();
		}

		@Override
		public Column next()
		{
			return new ColumnImpl(namespace, table, iter.next());
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

	}

	NameFilter<Column> findColumns( String columnNamePattern );

	Catalog getCatalog();

	Column getColumn( int idx );

	Column getColumn( String name );

	Iterator<? extends Column> getColumns();

	String getDBName();

	/**
	 * The schema the table belongs in.
	 * 
	 * @return
	 */
	Schema getSchema();

	/**
	 * Get the type of table.
	 * Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
	 * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 */
	String getType();
}
