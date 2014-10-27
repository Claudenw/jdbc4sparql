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

public interface TableDef
{

	public int getColumnCount();

	public ColumnDef getColumnDef( int idx );

	/**
	 * Get the list of columns in the table
	 *
	 * @return
	 */
	public List<ColumnDef> getColumnDefs();

	public int getColumnIndex( ColumnDef column );

	/**
	 * get the primary key for the table
	 *
	 * @return
	 */

	public Key getPrimaryKey();

	/**
	 * Get the table sort order key.
	 * returns null if the table is not sorted.
	 *
	 * @return
	 */

	public Key getSortKey();

	public TableDef getSuperTableDef();

}
