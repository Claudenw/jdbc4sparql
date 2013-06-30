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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jena.entities.annotations.Predicate;

public class SparqlView implements Table
{
	private String name;
	private SparqlQueryBuilder builder;
	
	public static final String NAME_SPACE = "http://org.xenei.jdbc4sparql/vocab#View";

	public SparqlView( final SparqlQueryBuilder builder )
	{
		this.builder = builder;
		this.name=NameUtils.createUUIDName();
	}
	
	public SparqlResultSet getResultSet() throws SQLException
	{
		return new SparqlResultSet(this, builder.build());
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public void delete()
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public NameFilter<Column> findColumns( String columnNamePattern )
	{
		return new NameFilter<Column>(columnNamePattern, builder.getResultColumns());
	}

	@Override
	public Catalog getCatalog()
	{
		return builder.getCatalog();
	}

	@Override
	public Column getColumn( int idx )
	{
		return builder.getResultColumns().get(idx);
	}

	@Override
	public Column getColumn( String name )
	{
		for (Column c : builder.getResultColumns())
		{
			if (c.getName().equals( name ))
			{
				return c;
			}
		}
		return null;
	}

	@Override
	public int getColumnCount()
	{
		return builder.getResultColumns().size();
	}

	@Override
	public int getColumnIndex( Column column )
	{
		for (int i=0;i<builder.getResultColumns().size();i++)
		{
			if (builder.getResultColumns().get(i).equals( column))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public int getColumnIndex( String columnName )
	{
		for (int i=0;i<builder.getResultColumns().size();i++)
		{
			if (builder.getResultColumns().get(i).getName().equals( columnName))
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public Iterator<? extends Column> getColumns()
	{
		return builder.getResultColumns().iterator();
	}

	@Override
	public Schema getSchema()
	{
		return builder.getCatalog().getViewSchema();
	}

	@Override
	public String getSPARQLName()
	{
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName()
	{
		return NameUtils.getDBName(this);
	}

	@Override
	public Table getSuperTable()
	{
		return null;
	}

	@Override
	public TableDef getTableDef()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getType()
	{
		return "VIEW";
	}

	@Override
	public String getRemarks()
	{
		return "";
	}

}
