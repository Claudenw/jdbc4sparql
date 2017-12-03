package org.xenei.jdbc4sparql.impl.rdf;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.xenei.jena.entities.SubjectInfo;
import org.xenei.jena.entities.annotations.Predicate;
import org.xenei.jena.entities.annotations.Subject;


@Subject(namespace = "http://org.xenei.jdbc4sparql/entity/Schema#")
public class RdfSchema extends RdfNamespacedObject implements Schema,
ResourceWrapper {
	public static class Builder implements Schema {
		public static RdfSchema fixupCatalog(EntityManager mgr,final RdfCatalog catalog,
				final RdfSchema schema) {
			schema.catalog = catalog;
			final Property p = ResourceFactory.createProperty(
					ResourceBuilder.getNamespace(mgr,RdfCatalog.class), "schemas");
			catalog.getResource().addProperty(p, schema.getResource());
			return schema;
		}

		private String name;
		private RdfCatalog catalog;

		private final Set<Table> tables = new HashSet<Table>();

		public RdfSchema build() {
			checkBuildState();
			EntityManager entityManager = catalog.getEntityManager();
			final Class<?> typeClass = RdfSchema.class;
			final String fqName = getFQName(entityManager);
			final ResourceBuilder builder = new ResourceBuilder(entityManager);

			Resource schema = null;
			if (builder.hasResource(fqName)) {
				schema = builder.getResource(fqName, typeClass);
			}
			else {
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
				retval = Builder.fixupCatalog(entityManager,catalog, retval);
				retval.getResource().getModel().register(retval.new ChangeListener());
				if (LOG.isDebugEnabled())
				{
					retval.getResource().listProperties().forEachRemaining( stmt -> LOG.debug( "build result: "+stmt ));
				}
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

		private String getFQName(EntityManager entityManager) {
			final StringBuilder sb = new StringBuilder()
			.append(catalog.getResource().getURI()).append(" ")
			.append(name);

			return String
					.format("%s/instance/N%s", ResourceBuilder
							.getFQName(entityManager,RdfSchema.class),

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
			super(RdfSchema.this, RdfSchema.class, "tables",
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
	private static Logger LOG = LoggerFactory.getLogger(RdfSchema.class);
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
			tableList.add(RdfTable.Builder.fixupSchema(getEntityManager(), this, (RdfTable) table));
		}
		this.tableList = tableList;
		return tableList;
	}

	@Override
	public RdfCatalog getCatalog() {
		if (catalog == null) {
			final Property p = ResourceFactory.createProperty(
					ResourceBuilder.getNamespace(getEntityManager(),RdfCatalog.class), "schemas");
			Resource r = this.getResource();
			r = r.getModel().listSubjectsWithProperty(p, r).next();
			
			try {
				catalog = getEntityManager().read(r, RdfCatalog.class);
			} catch (final MissingAnnotation e) {
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
			String localName = getLocalSchemaName();
			if (localName == null)
			{
				localName = getLocalSchemaName();
				getResource().listProperties().forEachRemaining( stmt -> System.out.println( stmt ));
			}
			System.out.println( "LocalName: "+localName);
			schemaName = new SchemaName(getCatalog().getShortName(),
					localName );
		}
		return schemaName;
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

	@Override
	public String toString() {
		return getName().toString();
	}
}
