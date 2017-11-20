package org.xenei.jdbc4sparql.impl.rdf;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Key;
import org.xenei.jdbc4sparql.iface.TableDef;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/TableDef#")
public class RdfTableDef extends RdfNamespacedObject implements TableDef,
ResourceWrapper {
	public static class Builder implements TableDef {
		private boolean distinct = false;
		private final List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
		private RdfKey primaryKey;
		private RdfKey sortKey;
		private RdfTableDef superTable;
		private final Class<? extends RdfTableDef> typeClass = RdfTableDef.class;
		private final Class<? extends RdfColumnDef> colDefClass = RdfColumnDef.class;

		public Builder addColumnDef(final ColumnDef column) {
			if (column instanceof ResourceWrapper) {
				columnDefs.add(column);
				return this;
			}
			throw new IllegalArgumentException(
					"ColumnDef must implement ResourceWrapper");
		}

		public RdfTableDef build(final Model model) {
			checkBuildState();

			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource tableDef = null;
			if (builder.hasResource(fqName)) {
				tableDef = builder.getResource(fqName, typeClass);
			}
			else {

				tableDef = builder.getResource(fqName, typeClass);

				if (primaryKey != null) {
					tableDef.addProperty(
							builder.getProperty(typeClass, "primaryKey"),
							primaryKey.getResource());
				}

				if (sortKey != null) {
					tableDef.addProperty(
							builder.getProperty(typeClass, "sortKey"),
							sortKey.getResource());

				}

				if (superTable != null) {
					tableDef.addProperty(
							builder.getProperty(typeClass, "superTableDef"),
							superTable.getResource());

				}

				tableDef.addLiteral(builder.getProperty(typeClass, "distinct"),
						distinct);

				RDFList lst = null;

				for (final ColumnDef seg : columnDefs) {
					final Resource s = ((ResourceWrapper) seg).getResource();
					if (lst == null) {
						lst = model.createList().with(s);
					}
					else {
						lst.add(s);
					}
				}
				final Property p = model.createProperty(ResourceBuilder
						.getFQName(colDefClass));
				tableDef.addProperty(p, lst);

			}
			try {
				return entityManager.read(tableDef, typeClass);
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		protected void checkBuildState() {
			if (columnDefs.size() == 0) {
				throw new IllegalStateException(
						"There must be at least one column defined");
			}

		}

		@Override
		public int getColumnCount() {
			return columnDefs.size();
		}

		@Override
		public ColumnDef getColumnDef(final int idx) {
			return columnDefs.get(idx);
		}

		@Override
		public List<ColumnDef> getColumnDefs() {
			return columnDefs;
		}

		@Override
		public int getColumnIndex(final ColumnDef column) {
			return columnDefs.indexOf(column);
		}

		public String getFQName() {
			final StringBuilder sb = new StringBuilder();
			for (final ColumnDef cd : columnDefs) {
				sb.append(((ResourceWrapper) cd).getResource().getURI())
				.append(" ");
			}
			if (primaryKey != null) {
				sb.append(primaryKey.getId()).append(" ");
			}
			if (sortKey != null) {
				sb.append(sortKey.getId()).append(" ");
			}
			if (superTable != null) {
				sb.append(superTable.getResource().getURI());
			}

			return String.format("%s/instance/UUID-%s",
					ResourceBuilder.getFQName(RdfTableDef.class),
					UUID.nameUUIDFromBytes(sb.toString().getBytes()));

		}

		@Override
		public RdfKey getPrimaryKey() {
			return primaryKey;
		}

		@Override
		public Key getSortKey() {
			return sortKey;
		}

		@Override
		public TableDef getSuperTableDef() {
			return superTable;
		}

		public void setDistinct(final boolean distinct) {
			this.distinct = distinct;
		}

		public Builder setPrimaryKey(final RdfKey key) {
			if (!key.isUnique()) {
				throw new IllegalArgumentException(
						"primary key must be a unique key");
			}
			this.primaryKey = key;
			return this;
		}

		public Builder setSortKey(final RdfKey key) {
			this.sortKey = key;
			return this;
		}

		public Builder setSuperTableDef(final RdfTableDef tableDef) {
			this.superTable = tableDef;
			return this;
		}

	}

	private List<ColumnDef> columns;

	private final Class<? extends RdfColumnDef> colDefClass = RdfColumnDef.class;

	@Override
	public int getColumnCount() {
		return getColumnDefs().size();
	}

	@Override
	public ColumnDef getColumnDef(final int idx) {
		return getColumnDefs().get(idx);
	}

	@Override
	public List<ColumnDef> getColumnDefs() {

		if (columns == null) {
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			columns = new ArrayList<ColumnDef>();
			final Resource resource = getResource();
			final Property p = resource.getModel().createProperty(
					ResourceBuilder.getFQName(RdfColumnDef.class));
			final List<RDFNode> resLst = resource.getRequiredProperty(p)
					.getResource().as(RDFList.class).asJavaList();
			for (final RDFNode n : resLst) {
				try {
					columns.add(entityManager.read(n.asResource(), colDefClass));
				} catch (final MissingAnnotation e) {
					throw new RuntimeException(e);
				}
			}
		}
		return columns;

	}

	@Override
	public int getColumnIndex(final ColumnDef column) {
		return getColumnDefs().indexOf(column);
	}

	/**
	 * get the primary key for the table
	 *
	 * @return
	 */
	@Override
	@Predicate(impl = true)
	public RdfKey getPrimaryKey() {
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
	
	/**
	 * Get the table sort order key. returns null if the table is not sorted.
	 *
	 * @return
	 */
	@Override
	@Predicate(impl = true)
	public RdfKey getSortKey() {
		throw new EntityManagerRequiredException();
	}

	@Override
	@Predicate(impl = true)
	public RdfTableDef getSuperTableDef() {
		throw new EntityManagerRequiredException();
	}

	@Predicate(impl = true)
	public boolean isDistinct() {
		throw new EntityManagerRequiredException();
	}

}
