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

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.impl.ColumnDefImpl;

public class SparqlColumnDef extends ColumnDefImpl
{
	public static class Builder extends ColumnDefImpl.Builder
	{
		private final List<String> querySegments = new ArrayList<String>();

		public Builder addQuerySegment( final String querySegment )
		{
			querySegments.add(querySegment);
			return this;
		}

		@Override
		public SparqlColumnDef build()
		{
			checkBuildState();
			final SparqlColumnDef columnDef = new SparqlColumnDef(
					getNamespace(), getLocalName(), getDisplaySize(),
					getType(), getPrecision(), getScale(), isSigned(),
					getNullable(), getLabel(), getTypeName(),
					getColumnClassName(), isAutoIncrement(), isCaseSensitive(),
					isCurrency(), isDefinitelyWritable(), isReadOnly(),
					isSearchable(), isWritable(), querySegments);
			resetVars();
			return columnDef;
		}

		@Override
		protected void checkBuildState()
		{
			super.checkBuildState();
			if (querySegments.size() == 0)
			{
				throw new IllegalStateException(
						"At least one query segment must be defined");
			}
		}

		@Override
		protected void resetVars()
		{
			super.resetVars();
			querySegments.clear();
		}

	}

	/**
	 * Query segments are string format strings where
	 * %1$s = table variable name
	 * %2$s = column variable name
	 * %3$s = column FQ name
	 * Multiple lines may be added. They will be added to the sparql query when
	 * the table is used.
	 * The string must have the form of a triple: S P O
	 * the components of the triple other than %1$s and %2$s must be fully
	 * qualified.
	 */
	private final List<String> querySegments;

	private SparqlColumnDef( final String namespace, final String localName,
			final int displaySize, final int type, final int precision,
			final int scale, final boolean signed, final int nullable,
			final String label, final String typeName,
			final String columnClassName, final boolean autoIncrement,
			final boolean caseSensitive, final boolean currency,
			final boolean definitelyWritable, final boolean readOnly,
			final boolean searchable, final boolean writable,
			final List<String> querySegments )
	{
		super(namespace, localName, displaySize, type, precision, scale,
				signed, nullable, label, typeName, columnClassName,
				autoIncrement, caseSensitive, currency, definitelyWritable,
				readOnly, searchable, writable);
		this.querySegments = new ArrayList<String>(querySegments);
	}

	public List<String> getQuerySegments()
	{
		return querySegments;
	}

}
