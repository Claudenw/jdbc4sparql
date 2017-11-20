package org.xenei.jdbc4sparql.impl.rdf;

import java.sql.DatabaseMetaData;
import java.sql.SQLDataException;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;


@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/ColumnDef#")
public class RdfColumnDef implements ColumnDef, ResourceWrapper {
	/**
	 * Column builder, by default columns are not nullable.
	 */
	public static class Builder implements ColumnDef {
		public static String getFQName(final ColumnDef colDef) {
			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(RdfColumnDef.class),
					ColumnDef.Util.createID(colDef));
		}

		public static Builder getIntegerBuilder() {
			return new Builder().setType(Types.INTEGER).setSigned(true);
		}

		public static Builder getSmallIntBuilder() {
			return new Builder().setType(Types.SMALLINT).setSigned(true);
		}

		public static Builder getStringBuilder() {
			return new Builder().setType(Types.VARCHAR).setSigned(false);
		}

		private String columnClassName = "";
		private int displaySize = 0;
		private Integer type = null;
		private Class<?> javaType = null;
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

		private final Class<? extends RdfColumnDef> typeClass = RdfColumnDef.class;

		public RdfColumnDef build(final EntityManager entityManager) {
			checkBuildState();

			final String fqName = Builder.getFQName(this);
			final ResourceBuilder builder = new ResourceBuilder(entityManager);
			Resource columnDef = null;
			if (builder.hasResource(fqName)) {
				columnDef = builder.getResource(fqName, typeClass);
			}
			else {
				columnDef = builder.getResource(fqName, typeClass);

				columnDef.addLiteral(
						builder.getProperty(typeClass, "displaySize"),
						displaySize);
				columnDef.addLiteral(builder.getProperty(typeClass, "type"),
						type);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "precision"), precision);
				columnDef.addLiteral(builder.getProperty(typeClass, "scale"),
						scale);
				columnDef.addLiteral(builder.getProperty(typeClass, "signed"),
						signed);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "nullable"), nullable);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "typeName"),
						StringUtils.defaultString(typeName,
								javaType.getSimpleName()));
				columnDef.addLiteral(
						builder.getProperty(typeClass, "columnClassName"),
						columnClassName);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "autoIncrement"),
						autoIncrement);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "caseSensitive"),
						caseSensitive);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "currency"), currency);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "definitelyWritable"),
						definitelyWritable);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "readOnly"), readOnly);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "searchable"),
						searchable);
				columnDef.addLiteral(
						builder.getProperty(typeClass, "writable"), writable);

			}

			
			try {
				return entityManager.read(columnDef, typeClass);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		protected void checkBuildState() {
			if (type == null) {
				throw new IllegalStateException("type must be set");
			}
		}

		@Override
		public String getColumnClassName() {
			return columnClassName;
		}

		@Override
		public int getDisplaySize() {
			return displaySize;
		}

		@Override
		public int getNullable() {
			return nullable;
		}

		@Override
		public int getPrecision() {
			return precision;
		}

		@Override
		public int getScale() {
			return scale;
		}

		@Override
		public int getType() {
			return type;
		}

		@Override
		public String getTypeName() {
			return typeName;
		}

		@Override
		public boolean isAutoIncrement() {
			return autoIncrement;
		}

		@Override
		public boolean isCaseSensitive() {
			return caseSensitive;
		}

		@Override
		public boolean isCurrency() {
			return currency;
		}

		@Override
		public boolean isDefinitelyWritable() {
			return definitelyWritable;
		}

		@Override
		public boolean isReadOnly() {
			return readOnly;
		}

		@Override
		public boolean isSearchable() {
			return searchable;
		}

		@Override
		public boolean isSigned() {
			return signed;
		}

		@Override
		public boolean isWritable() {
			return writable;
		}

		public Builder setAutoIncrement(final boolean autoIncrement) {
			this.autoIncrement = autoIncrement;
			return this;
		}

		public Builder setCaseSensitive(final boolean caseSensitive) {
			this.caseSensitive = caseSensitive;
			return this;
		}

		public Builder setColumnClassName(final String columnClassName) {
			this.columnClassName = columnClassName;
			return this;
		}

		public Builder setCurrency(final boolean currency) {
			this.currency = currency;
			return this;
		}

		public Builder setDefinitelyWritable(final boolean definitelyWritable) {
			this.definitelyWritable = definitelyWritable;
			return this;
		}

		public Builder setDisplaySize(final int displaySize) {
			this.displaySize = displaySize;
			return this;
		}

		public Builder setNullable(final int nullable) {
			this.nullable = nullable;
			return this;
		}

		public Builder setPrecision(final int precision) {
			this.precision = precision;
			return this;
		}

		public Builder setReadOnly(final boolean readOnly) {
			this.readOnly = readOnly;
			return this;
		}

		public Builder setScale(final int scale) {
			this.scale = scale;
			return this;
		}

		public Builder setSearchable(final boolean searchable) {
			this.searchable = searchable;
			return this;
		}

		public Builder setSigned(final boolean signed) {
			this.signed = signed;
			return this;
		}

		public Builder setType(final int type) {
			this.type = type;
			try {
				this.javaType = TypeConverter.getJavaType(type);
			} catch (final SQLDataException e) {
				throw new IllegalArgumentException(String.format(
						"SQL type %s is not supported", type));
			}
			return this;
		}

		public Builder setTypeName(final String typeName) {
			this.typeName = typeName;
			return this;
		}

		public Builder setWritable(final boolean writable) {
			this.writable = writable;
			return this;
		}

	}

	@Override
	@Predicate(impl = true)
	public String getColumnClassName() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public int getDisplaySize() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public int getNullable() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public int getPrecision() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public Resource getResource() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public EntityManager getEntityManager() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public SubjectInfo getSubjectInfo() {
		throw new EntityManagerRequiredException();
	}
	
	@Override
	@Predicate(impl = true)
	public int getScale() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public int getType() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public String getTypeName() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isAutoIncrement() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isCaseSensitive() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isCurrency() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isDefinitelyWritable() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isReadOnly() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isSearchable() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isSigned() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public boolean isWritable() {
		throw new EntityManagerRequiredException();
	}
}
