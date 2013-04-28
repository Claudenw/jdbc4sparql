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

import java.util.List;

public interface TableDef extends NamespacedObject
{

	int getColumnCount();

	ColumnDef getColumnDef( int idx );

	ColumnDef getColumnDef( String name );

	/**
	 * get the name for the table.
	 * 
	 * @return
	 */
	// String getName();
	/**
	 * Get the list of columns in the table
	 * 
	 * @return
	 */
	List<ColumnDef> getColumnDefs();

	int getColumnIndex( ColumnDef column );

	int getColumnIndex( String columnName );

	/**
	 * get the primary key for the table
	 * 
	 * @return
	 */
	Key getPrimaryKey();

	/**
	 * Get the table sort order key.
	 * returns null if the table is not sorted.
	 * 
	 * @return
	 */
	Key getSortKey();

	TableDef getSuperTableDef();

	/**
	 * Verify that the row data matches the definition
	 * 
	 * @param row
	 * @throws IllegalArgumentException
	 *             on errors
	 */
	void verify( Object[] row );
}
