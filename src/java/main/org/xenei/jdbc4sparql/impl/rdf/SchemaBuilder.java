package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.NameFilter;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.annotations.Predicate;

public class SchemaBuilder implements Schema
{
	private String name;
	private Catalog catalog;
	private final Set<Table> tables = new HashSet<Table>();

	public Schema build( final Model model )
	{
		checkBuildState();
		final Class<?> typeClass = RdfSchema.class;
		final String fqName = getFQName();
		final ResourceBuilder builder = new ResourceBuilder(model);

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();

		Resource schema = null;
		if (builder.hasResource(fqName))
		{
			schema = builder.getResource(fqName, typeClass);
		}
		else
		{
			schema = builder.getResource(fqName, typeClass);
			schema.addLiteral(RDFS.label, name);

			for (final Table tbl : tables)
			{
				schema.addProperty(builder.getProperty(typeClass, "table"),
						tbl.getResource());
			}
		}

		try
		{
			final RdfSchema retval = entityManager
					.read(schema, RdfSchema.class);
			catalog.getResource().addProperty(
					builder.getProperty(RdfCatalog.class, "schemas"), schema);
			model.register(retval.new ChangeListener());
			return retval;
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	private void checkBuildState()
	{
		if (StringUtils.isBlank(name))
		{
			throw new IllegalStateException("Name must be set");
		}

		if (catalog == null)
		{
			throw new IllegalStateException("catalog must be set");
		}

	}

	@Override
	public NameFilter<Table> findTables( final String tableNamePattern )
	{
		return new NameFilter<Table>(tableNamePattern, tables);
	}

	@Override
	public Catalog getCatalog()
	{
		return catalog;
	}

	private String getFQName()
	{
		final StringBuilder sb = new StringBuilder()
				.append(catalog.getResource().getURI()).append(" ")
				.append(name);

		return String.format("%s/instance/N%s",
				ResourceBuilder.getFQName(RdfSchema.class),

				UUID.nameUUIDFromBytes(sb.toString().getBytes()).toString());
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	@Predicate
	public Resource getResource()
	{
		return ResourceFactory.createResource(getFQName());
	}

	@Override
	public Table getTable( final String tableName )
	{
		final NameFilter<Table> nf = findTables(tableName);
		return nf.hasNext() ? nf.next() : null;
	}

	@Override
	public Set<Table> getTables()
	{
		return tables;
	}

	public SchemaBuilder setCatalog( final Catalog catalog )
	{
		this.catalog = catalog;
		return this;
	}

	public SchemaBuilder setName( final String name )
	{
		this.name = name;
		return this;
	}
}
