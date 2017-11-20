package org.xenei.jdbc4sparql.impl.rdf;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.RDFS;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.ColumnName;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;


@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Column#")
public class RdfColumn extends RdfNamespacedObject implements Column,
ResourceWrapper {
	public static class Builder implements Column {
		public static RdfColumn fixupTable(final RdfTable table,
				final RdfColumn column) {
			column.table = table;
			return column;
		}

		private ColumnDef columnDef;
		private Table table;
		private String name;
		private final Class<? extends RdfColumn> typeClass = RdfColumn.class;
		private String remarks = null;

		private final List<String> querySegments = new ArrayList<String>();

		/**
		 * Add a query segment where %1$s = tableVar, and %2$s=columnVar
		 *
		 * @param querySegment
		 * @return
		 */
		public Builder addQuerySegment(final String querySegment) {
			querySegments.add(querySegment);
			return this;
		}

		public RdfColumn build(final EntityManager entityManager) {
			checkBuildState();

			final ResourceBuilder builder = new ResourceBuilder(entityManager);

			Resource column = null;
			if (builder.hasResource(getFQName(entityManager))) {
				column = builder.getResource(getFQName(entityManager), typeClass);
			}
			else {
				column = builder.getResource(getFQName(entityManager), typeClass);

				column.addLiteral(RDFS.label, name);
				column.addLiteral(builder.getProperty(typeClass, "remarks"),
						StringUtils.defaultString(remarks));
				column.addProperty(builder.getProperty(typeClass, "columnDef"),
						((ResourceWrapper) columnDef).getResource());

			}

			if (!querySegments.isEmpty()) {
				final String eol = System.getProperty("line.separator");
				final StringBuilder sb = new StringBuilder();
				for (final String seg : querySegments) {
					sb.append(seg).append(eol);
				}

				final Property querySegmentProp = builder.getProperty(
						typeClass, "querySegmentFmt");
				column.addLiteral(querySegmentProp, getQuerySegmentFmt());
				querySegments.clear();
			}

			
			try {
				final RdfColumn retval = entityManager.read(column, typeClass);
				RdfTable tbl = null;
				if (table instanceof RdfTable.Builder) {
					tbl = ((RdfTable.Builder) table).build(entityManager);
				}
				else if (table instanceof RdfTable) {
					tbl = (RdfTable) table;
				}
				else {
					throw new IllegalArgumentException(
							"table not an rdf table or builder");
				}
				// table is now a real RdfTable so add it as a property
				column.addProperty(builder.getProperty(typeClass, "table"),
						tbl.getResource());
				return Builder.fixupTable(tbl, retval);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		protected void checkBuildState() {
			if (columnDef == null) {
				throw new IllegalStateException("columnDef must be set");
			}

			if (!(columnDef instanceof ResourceWrapper)) {
				throw new IllegalStateException(
						"columnDef must implement ResourceWrapper");
			}

			if (table == null) {
				throw new IllegalStateException("table must be set");
			}

			if (StringUtils.isBlank(name)) {
				throw new IllegalStateException("Name must be set");
			}

			if (querySegments.size() == 0) {
				querySegments.add("# no query segments provided");
			}

		}

		@Override
		public Catalog getCatalog() {
			return table.getSchema().getCatalog();
		}

		@Override
		public ColumnDef getColumnDef() {
			return columnDef;
		}

		public String getFQName(EntityManager mgr) {
			final ResourceWrapper rw = (ResourceWrapper) getColumnDef();
			final StringBuilder sb = new StringBuilder()
			.append(rw.getResource().getURI()).append(" ").append(name)
			.append(" ").append(getQuerySegmentFmt());
			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(mgr, RdfColumn.class),
					UUID.nameUUIDFromBytes(sb.toString().getBytes()));
		}

		@Override
		public ColumnName getName() {
			return table.getName().getColumnName(name);
		}

		@Override
		public String getQuerySegmentFmt() {
			final String eol = System.getProperty("line.separator");
			final StringBuilder sb = new StringBuilder();
			if (!querySegments.isEmpty()) {

				for (final String seg : querySegments) {
					sb.append(seg).append(eol);
				}
			}
			else {
				sb.append("# no segment provided").append(eol);
			}
			return sb.toString();
		}

		@Override
		public String getRemarks() {
			return remarks;
		}

		@Override
		public Schema getSchema() {
			return table.getSchema();
		}

		@Override
		public String getSPARQLName() {
			return NameUtils.getSPARQLName(this);
		}

		@Override
		public String getSQLName() {
			return NameUtils.getDBName(this);
		}

		@Override
		public Table getTable() {
			return table;
		}

		@Override
		public boolean hasQuerySegments() {
			return querySegments.size() > 0;
		}

		@Override
		public boolean isOptional() {
			return getColumnDef().getNullable() != DatabaseMetaData.columnNoNulls;
		}

		public Builder setColumnDef(final ColumnDef columnDef) {
			if (columnDef instanceof ResourceWrapper) {
				this.columnDef = columnDef;
			}
			return this;
		}

		public Builder setName(final String name) {
			this.name = name;
			return this;
		}

		public Builder setRemarks(final String remarks) {
			this.remarks = remarks;
			return this;
		}

		public Builder setTable(final Table table) {
			this.table = table;
			return this;
		}
	}

	private RdfTable table;
	private ColumnName columnName;

	public void delete() {
		final Model model = getResource().getModel();
		final Resource r = getResource();
		model.enterCriticalSection(Lock.WRITE);
		try {
			model.remove(null, null, r);
			model.remove(r, null, null);
		} finally {
			model.leaveCriticalSection();
		}
	}

	@Override
	public RdfCatalog getCatalog() {
		return getSchema().getCatalog();
	}

	@Override
	@Predicate(impl = true)
	public RdfColumnDef getColumnDef() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public ColumnName getName() {
		if (columnName == null) {
			columnName = getTable().getName().getColumnName(getSimpleName());
		}
		return columnName;
	}

	@Override
	@Predicate(impl = true)
	public String getQuerySegmentFmt() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public String getRemarks() {
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
	public RdfSchema getSchema() {
		return getTable().getSchema();
	}

	@Predicate(impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label")
	public String getSimpleName() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public String getSPARQLName() {
		return NameUtils.getSPARQLName(this);
	}

	@Override
	public String getSQLName() {
		return NameUtils.getDBName(this);
	}

	@Override
	public RdfTable getTable() {
		return table;
	}

	@Override
	public boolean hasQuerySegments() {
		return true;
	}

	@Override
	public boolean isOptional() {
		return getColumnDef().getNullable() != DatabaseMetaData.columnNoNulls;
	}

	@Override
	public String toString() {
		return getName().toString();
	}
}
