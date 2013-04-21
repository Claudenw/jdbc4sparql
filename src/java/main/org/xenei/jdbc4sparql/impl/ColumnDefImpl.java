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
package org.xenei.jdbc4sparql.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.DatabaseMetaData;
import java.sql.Types;

import org.apache.commons.codec.binary.Hex;
import org.xenei.jdbc4sparql.iface.ColumnDef;

public class ColumnDefImpl extends NamespaceImpl implements ColumnDef
{

	public static class Builder
	{
		public static Builder getIntegerBuilder( final String namespace,
				final String localName )
		{
			return new Builder().setNamespace(namespace)
					.setLocalName(localName).setType(Types.INTEGER)
					.setSigned(true);
		}

		public static Builder getSmallIntBuilder( final String namespace,
				final String localName )
		{
			return new Builder().setNamespace(namespace)
					.setLocalName(localName).setType(Types.SMALLINT)
					.setSigned(false);
		}

		public static Builder getStringBuilder( final String namespace,
				final String localName )
		{
			return new Builder().setNamespace(namespace)
					.setLocalName(localName).setType(Types.VARCHAR)
					.setSigned(false);
		}

		private String namespace;
		private String localName;
		private String columnClassName = "";
		private int displaySize = 0;
		private Integer type = null;
		private int precision = 0;
		private int scale = 0;
		private boolean signed = false;
		private int nullable = DatabaseMetaData.columnNoNulls;
		private String label;
		private String typeName = "";
		private boolean autoIncrement = false;
		private boolean caseSensitive = false;
		private boolean currency = false;
		private boolean definitelyWritable = false;

		private boolean readOnly = false;

		private boolean searchable = false;

		private boolean writable = false;

		public Builder()
		{
		}

		public ColumnDefImpl build()
		{
			checkBuildState();
			final ColumnDefImpl columnDef = new ColumnDefImpl(namespace,
					localName, displaySize, type, precision, scale, signed,
					nullable, label, typeName, columnClassName, autoIncrement,
					caseSensitive, currency, definitelyWritable, readOnly,
					searchable, writable);
			resetVars();
			return columnDef;
		}

		protected void checkBuildState()
		{
			if (namespace == null)
			{
				throw new IllegalStateException("namespace must be set");
			}
			if (localName == null)
			{
				throw new IllegalStateException("localName must be set");
			}
			if (type == null)
			{
				throw new IllegalStateException("type must be set");
			}
		}

		public String getColumnClassName()
		{
			return columnClassName;
		}

		public int getDisplaySize()
		{
			return displaySize;
		}

		public String getLabel()
		{
			return label;
		}

		public String getLocalName()
		{
			return localName;
		}

		public String getNamespace()
		{
			return namespace;
		}

		public int getNullable()
		{
			return nullable;
		}

		public int getPrecision()
		{
			return precision;
		}

		public int getScale()
		{
			return scale;
		}

		public int getType()
		{
			return type;
		}

		public String getTypeName()
		{
			return typeName;
		}

		public boolean isAutoIncrement()
		{
			return autoIncrement;
		}

		public boolean isCaseSensitive()
		{
			return caseSensitive;
		}

		public boolean isCurrency()
		{
			return currency;
		}

		public boolean isDefinitelyWritable()
		{
			return definitelyWritable;
		}

		public boolean isReadOnly()
		{
			return readOnly;
		}

		public boolean isSearchable()
		{
			return searchable;
		}

		public boolean isSigned()
		{
			return signed;
		}

		public boolean isWritable()
		{
			return writable;
		}

		protected void resetVars()
		{
			localName = null;
			label = null;
		}

		public Builder setAutoIncrement( final boolean autoIncrement )
		{
			this.autoIncrement = autoIncrement;
			return this;
		}

		public Builder setCaseSensitive( final boolean caseSensitive )
		{
			this.caseSensitive = caseSensitive;
			return this;
		}

		public Builder setColumnClassName( final String columnClassName )
		{
			this.columnClassName = columnClassName;
			return this;
		}

		public Builder setCurrency( final boolean currency )
		{
			this.currency = currency;
			return this;
		}

		public Builder setDefinitelyWritable( final boolean definitelyWritable )
		{
			this.definitelyWritable = definitelyWritable;
			return this;
		}

		public Builder setDisplaySize( final int displaySize )
		{
			this.displaySize = displaySize;
			return this;
		}

		public Builder setLabel( final String label )
		{
			this.label = label;
			return this;
		}

		public Builder setLocalName( final String localName )
		{
			this.localName = localName;
			if (this.label == null)
			{
				this.label = localName;
			}
			return this;
		}

		public Builder setNamespace( final String namespace )
		{
			this.namespace = namespace;
			return this;
		}

		public Builder setNullable( final int nullable )
		{
			this.nullable = nullable;
			return this;
		}

		public Builder setPrecision( final int precision )
		{
			this.precision = precision;
			return this;
		}

		public Builder setReadOnly( final boolean readOnly )
		{
			this.readOnly = readOnly;
			return this;
		}

		public Builder setScale( final int scale )
		{
			this.scale = scale;
			return this;
		}

		public Builder setSearchable( final boolean searchable )
		{
			this.searchable = searchable;
			return this;
		}

		public Builder setSigned( final boolean signed )
		{
			this.signed = signed;
			return this;
		}

		public Builder setType( final int type )
		{
			this.type = type;
			return this;
		}

		public Builder setTypeName( final String typeName )
		{
			this.typeName = typeName;
			return this;
		}

		public Builder setWritable( final boolean writable )
		{
			this.writable = writable;
			return this;
		}
	}

	private final int displaySize;
	private final int type;
	private final int precision;
	private final int scale;
	private final boolean signed;
	private final int nullable;
	private final String label;
	private final String typeName;
	private final String columnClassName;
	private final boolean autoIncrement;
	private final boolean caseSensitive;
	private final boolean currency;
	private final boolean definitelyWritable;
	private final boolean readOnly;
	private final boolean searchable;
	private final boolean writable;

	private final String id;

	protected ColumnDefImpl( final String namespace, final String localName,
			final int displaySize, final int type, final int precision,
			final int scale, final boolean signed, final int nullable,
			final String label, final String typeName,
			final String columnClassName, final boolean autoIncrement,
			final boolean caseSensitive, final boolean currency,
			final boolean definitelyWritable, final boolean readOnly,
			final boolean searchable, final boolean writable )
	{
		super(namespace, localName);
		this.displaySize = displaySize;
		this.type = type;
		this.precision = precision;
		this.scale = scale;
		this.signed = signed;
		this.nullable = nullable;
		this.label = label;
		this.typeName = typeName;
		this.columnClassName = columnClassName;
		this.autoIncrement = autoIncrement;
		this.caseSensitive = caseSensitive;
		this.currency = currency;
		this.definitelyWritable = definitelyWritable;
		this.readOnly = readOnly;
		this.searchable = searchable;
		this.writable = writable;

		final StringBuilder sb = new StringBuilder().append(displaySize)
				.append(type).append(precision).append(scale).append(signed)
				.append(nullable).append(label).append(typeName)
				.append(columnClassName).append(autoIncrement)
				.append(caseSensitive).append(currency)
				.append(definitelyWritable).append(readOnly).append(searchable)
				.append(writable);
		try
		{
			final MessageDigest digest = MessageDigest.getInstance("MD5");
			final byte[] bytes = digest.digest(sb.toString().getBytes());
			this.id = Hex.encodeHexString(bytes);
		}
		catch (final NoSuchAlgorithmException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals( final Object o )
	{
		if (o instanceof ColumnDefImpl)
		{
			return this.id.equals(((ColumnDefImpl) o).id);
		}
		return false;
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

	public String getId()
	{
		return this.id;
	}

	@Override
	public String getLabel()
	{
		return label;
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
	public int hashCode()
	{
		return this.id.hashCode();
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

	@Override
	public String toString()
	{
		return String.format("ColumnDef[%s]", getLabel());
	}
}