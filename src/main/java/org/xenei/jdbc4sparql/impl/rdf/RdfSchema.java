package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.name.SchemaName;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.EntityManagerRequiredException;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;

@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Schema#")
public class RdfSchema extends RdfNamespacedObject implements Schema,
		ResourceWrapper {
	public static class Builder implements Schema {
		public static RdfSchema fixupCatalog(final RdfCatalog catalog,
				final RdfSchema schema) {
			schema.catalog = catalog;
			final Property p = ResourceFactory.createProperty(
					ResourceBuilder.getNamespace(RdfCatalog.class), "schemas");
			catalog.getResource().addProperty(p, schema.getResource());
			return schema;
		}

		private String name;
		private RdfCatalog catalog;

		private final Set<Table> tables = new HashSet<Table>();

		public RdfSchema build(final Model model) {
			checkBuildState();
			final Class<?> typeClass = RdfSchema.class;
			final String fqName = getFQName();
			final ResourceBuilder builder = new ResourceBuilder(model);

			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();

			Resource schema = null;
			if (builder.hasResource(fqName)) {
				schema = builder.getResource(fqName, typeClass);
			} else {
				schema = builder.getResource(fqName, typeClass);
				schema.addLiteral(RDFS.label, name);

				for (final Table tbl : tables) {
					if (tbl instanceof ResourceWrapper) {
						schema.addProperty(
								builder.getProperty(typeClass, "table"),
								((ResourceWrapper) tbl).getResource());
					}
				}
			}

			try {
				RdfSchema retval = entityManager.read(schema, RdfSchema.class);
				retval = Builder.fixupCatalog(catalog, retval);
				model.register(retval.new ChangeListener());
				return retval;
			} catch (final MissingAnnotation e) {
				throw new RuntimeException(e);
			}
		}

		private void checkBuildState() {
			if (name == null) {
				throw new IllegalStateException("Name must be set");
			}

			if (catalog == null) {
				throw new IllegalStateException("catalog must be set");
			}

		}

		@Override
		public NameFilter<Table> findTables(final String tableNamePattern) {
			return new NameFilter<Table>(tableNamePattern, tables);
		}

		@Override
		public Catalog getCatalog() {
			return catalog;
		}

		private String getFQName() {
			final StringBuilder sb = new StringBuilder()
					.append(catalog.getResource().getURI()).append(" ")
					.append(name);

			return String
					.format("%s/instance/N%s", ResourceBuilder
							.getFQName(RdfSchema.class),

					UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString());
		}

		@Override
		public SchemaName getName() {
			return new SchemaName(catalog.getShortName(), name);
		}

		@Override
		public Table getTable(final String tableName) {
			final NameFilter<Table> nf = findTables(tableName);
			return nf.hasNext() ? nf.next() : null;
		}

		@Override
		public Set<Table> getTables() {
			return tables;
		}

		public Builder setCatalog(final RdfCatalog catalog) {
			this.catalog = catalog;
			return this;
		}

		public Builder setName(final String name) {
			this.name = name;
			return this;
		}

	}

	public class ChangeListener extends
			AbstractChangeListener<Schema, RdfTable> {
		public ChangeListener() {
			super(RdfSchema.this.getResource(), RdfSchema.class, "tables",
					RdfTable.class);
		}

		@Override
		protected void addObject(final RdfTable t) {
			tableList.add(t);
		}

		@Override
		protected void clearObjects() {
			tableList = null;
		}

		@Override
		protected boolean isListening() {
			return tableList != null;
		}

		@Override
		protected void removeObject(final RdfTable t) {
			tableList.remove(t);
		}

	}

	private RdfCatalog catalog;
	private SchemaName schemaName = null;

	private Set<Table> tableList;

	@Predicate(impl = true)
	public void addTables(final RdfTable table) {
		throw new EntityManagerRequiredException();
	}

	public void delete() {
		for (final Table tbl : readTables()) {
			tbl.delete();
		}

		final Model model = getResource().getModel();
		model.enterCriticalSection(Lock.WRITE);
		try {
			getResource().getModel().remove(null, null, getResource());
			getResource().getModel().remove(getResource(), null, null);
		} finally {
			model.leaveCriticalSection();
		}
	}

	@Override
	public NameFilter<Table> findTables(final String tableNamePattern) {
		return new NameFilter<Table>(tableNamePattern, readTables());
	}

	public Set<Table> fixupTables(final Set<Table> tables) {
		final Set<Table> tableList = new HashSet<Table>();
		for (final Table table : tables) {
			tableList.add(RdfTable.Builder.fixupSchema(this, (RdfTable) table));
		}
		this.tableList = tableList;
		return tableList;
	}

	@Override
	public RdfCatalog getCatalog() {
		if (catalog == null) {
			final Property p = ResourceFactory.createProperty(
					ResourceBuilder.getNamespace(RdfCatalog.class), "schemas");
			Resource r = this.getResource();
			r = r.getModel().listSubjectsWithProperty(p, r).next();
			final EntityManager entityManager = EntityManagerFactory
					.getEntityManager();
			try {
				catalog = entityManager.read(r, RdfCatalog.class);
			} catch (MissingAnnotation e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		return catalog;
	}

	@Predicate(impl = true, namespace = "http://www.w3.org/2000/01/rdf-schema#", name = "label")
	public String getLocalSchemaName() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public SchemaName getName() {
		if (schemaName == null) {
			schemaName = new SchemaName(getCatalog().getShortName(),
					getLocalSchemaName());
		}
		return schemaName;
	}

	@Override
	@Predicate(impl = true)
	public Resource getResource() {
		throw new EntityManagerRequiredException();
	}

	@Override
	public Table getTable(final String tableName) {
		final NameFilter<Table> nf = findTables(tableName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	@Predicate(impl = true, type = RdfTable.class, postExec = "fixupTables")
	public Set<Table> getTables() {
		throw new EntityManagerRequiredException();
	}

	private Set<Table> readTables() {
		if (tableList == null) {
			tableList = getTables();
		}
		return tableList;
	}

}
