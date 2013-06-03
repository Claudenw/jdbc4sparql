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
package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;

public class ColumnDefBuilder implements ColumnDef
{
	public static String getFQName( final ColumnDef colDef )
	{
		return String.format("%s/instance/UUID-%s",
				ResourceBuilder.getFQName(RdfColumnDef.class),
				ColumnDef.Util.createID(colDef));
	}

	public static ColumnDefBuilder getIntegerBuilder()
	{
		return new ColumnDefBuilder().setType(Types.INTEGER).setSigned(true);
	}

	public static ColumnDefBuilder getSmallIntBuilder()
	{
		return new ColumnDefBuilder().setType(Types.SMALLINT).setSigned(true);
	}

	public static ColumnDefBuilder getStringBuilder()
	{
		return new ColumnDefBuilder().setType(Types.VARCHAR).setSigned(false);
	}

	private String columnClassName = "";
	private int displaySize = 0;
	private Integer type = null;
	private int precision = 0;
	private int scale = 0;
	private boolean signed = false;
	private int nullable = DatabaseMetaData.columnNoNulls;
	private String typeName;
	private boolean autoIncrement = false;
	private boolean caseSensitive = false;
	private boolean currency = false;

	private boolean definitelyWritable = false;

	private boolean readOnly = false;

	private boolean searchable = false;

	private boolean writable = false;
	
	private Class<? extends RdfColumnDef> typeClass = RdfColumnDef.class;

	public ColumnDefBuilder()
	{	
	}
	
	protected ColumnDefBuilder( Class<? extends RdfColumnDef> typeClass ) {
		this.typeClass = typeClass;
	}
	
	public ColumnDef build( final Model model )
	{
		checkBuildState();

		
		final String fqName = ColumnDefBuilder.getFQName(this);
		final ResourceBuilder builder = new ResourceBuilder(model);
		Resource columnDef = null;
		if (builder.hasResource(fqName))
		{
			columnDef = builder.getResource(fqName, typeClass);
		}
		else
		{
			columnDef = builder.getResource(fqName, typeClass);

			columnDef.addLiteral(builder.getProperty(typeClass, "displaySize"),
					displaySize);
			columnDef.addLiteral(builder.getProperty(typeClass, "type"), type);
			columnDef.addLiteral(builder.getProperty(typeClass, "precision"),
					precision);
			columnDef
					.addLiteral(builder.getProperty(typeClass, "scale"), scale);
			columnDef.addLiteral(builder.getProperty(typeClass, "signed"),
					signed);
			columnDef.addLiteral(builder.getProperty(typeClass, "nullable"),
					nullable);
			columnDef.addLiteral(builder.getProperty(typeClass, "typeName"),
					StringUtils.defaultString(typeName, TypeConverter
							.getJavaType(type).getSimpleName()));
			columnDef.addLiteral(
					builder.getProperty(typeClass, "columnClassName"),
					columnClassName);
			columnDef.addLiteral(
					builder.getProperty(typeClass, "autoIncrement"),
					autoIncrement);
			columnDef.addLiteral(
					builder.getProperty(typeClass, "caseSensitive"),
					caseSensitive);
			columnDef.addLiteral(builder.getProperty(typeClass, "currency"),
					currency);
			columnDef.addLiteral(
					builder.getProperty(typeClass, "definitelyWritable"),
					definitelyWritable);
			columnDef.addLiteral(builder.getProperty(typeClass, "readOnly"),
					readOnly);
			columnDef.addLiteral(builder.getProperty(typeClass, "searchable"),
					searchable);
			columnDef.addLiteral(builder.getProperty(typeClass, "writable"),
					writable);

		}

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();
		try
		{
			return entityManager.read(columnDef, typeClass);
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	protected void checkBuildState()
	{
		if (type == null)
		{
			throw new IllegalStateException("type must be set");
		}
	}

	@Override
	public String getColumnClassName()
	{
		return columnClassName;
	}

	@Override
	public int getDisplaySize()
	{
		return displaySize;
	}

	@Override
	public int getNullable()
	{
		return nullable;
	}

	@Override
	public int getPrecision()
	{
		return precision;
	}

	@Override
	public Resource getResource()
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public int getScale()
	{
		return scale;
	}

	@Override
	public int getType()
	{
		return type;
	}

	@Override
	public String getTypeName()
	{
		return typeName;
	}

	@Override
	public boolean isAutoIncrement()
	{
		return autoIncrement;
	}

	@Override
	public boolean isCaseSensitive()
	{
		return caseSensitive;
	}

	@Override
	public boolean isCurrency()
	{
		return currency;
	}

	@Override
	public boolean isDefinitelyWritable()
	{
		return definitelyWritable;
	}

	@Override
	public boolean isReadOnly()
	{
		return readOnly;
	}

	@Override
	public boolean isSearchable()
	{
		return searchable;
	}

	@Override
	public boolean isSigned()
	{
		return signed;
	}

	@Override
	public boolean isWritable()
	{
		return writable;
	}

	public ColumnDefBuilder setAutoIncrement( final boolean autoIncrement )
	{
		this.autoIncrement = autoIncrement;
		return this;
	}

	public ColumnDefBuilder setCaseSensitive( final boolean caseSensitive )
	{
		this.caseSensitive = caseSensitive;
		return this;
	}

	public ColumnDefBuilder setColumnClassName( final String columnClassName )
	{
		this.columnClassName = columnClassName;
		return this;
	}

	public ColumnDefBuilder setCurrency( final boolean currency )
	{
		this.currency = currency;
		return this;
	}

	public ColumnDefBuilder setDefinitelyWritable(
			final boolean definitelyWritable )
	{
		this.definitelyWritable = definitelyWritable;
		return this;
	}

	public ColumnDefBuilder setDisplaySize( final int displaySize )
	{
		this.displaySize = displaySize;
		return this;
	}

	public ColumnDefBuilder setNullable( final int nullable )
	{
		this.nullable = nullable;
		return this;
	}

	public ColumnDefBuilder setPrecision( final int precision )
	{
		this.precision = precision;
		return this;
	}

	public ColumnDefBuilder setReadOnly( final boolean readOnly )
	{
		this.readOnly = readOnly;
		return this;
	}

	public ColumnDefBuilder setScale( final int scale )
	{
		this.scale = scale;
		return this;
	}

	public ColumnDefBuilder setSearchable( final boolean searchable )
	{
		this.searchable = searchable;
		return this;
	}

	public ColumnDefBuilder setSigned( final boolean signed )
	{
		this.signed = signed;
		return this;
	}

	public ColumnDefBuilder setType( final int type )
	{
		this.type = type;
		return this;
	}

	public ColumnDefBuilder setTypeName( final String typeName )
	{
		this.typeName = typeName;
		return this;
	}

	public ColumnDefBuilder setWritable( final boolean writable )
	{
		this.writable = writable;
		return this;
	}
}
