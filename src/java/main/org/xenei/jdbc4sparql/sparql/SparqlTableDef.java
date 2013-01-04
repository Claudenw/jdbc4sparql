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

import java.util.ArrayList;
import java.util.List;

import org.xenei.jdbc4sparql.impl.TableDefImpl;

public class SparqlTableDef extends TableDefImpl
{

	/**
	 * Query segments are string format strings where
	 * %1$s = table variable name
	 * %2$s = table FQ name.
	 * Multiple lines may be added. They will be added to the sparql query when
	 * the table is used.
	 * The string must have the form of a triple: S, P, O
	 * the components of the triple other than %1$s and %2$s must be fully
	 * qualified.
	 */
	private final List<String> querySegments;

	public SparqlTableDef( final String namespace, final String name,
			final String querySegment )
	{
		super(namespace, name);
		this.querySegments = new ArrayList<String>();
		this.querySegments.add(querySegment);
	}

	public void addQuerySegment( final String querySegment )
	{
		querySegments.add(querySegment);
	}

	public List<String> getQuerySegments()
	{
		return querySegments;
	}
}
