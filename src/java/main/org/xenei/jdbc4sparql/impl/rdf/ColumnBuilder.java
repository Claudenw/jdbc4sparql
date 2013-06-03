package org.xenei.jdbc4sparql.impl.rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jena.entities.EntityManager;
import org.xenei.jena.entities.EntityManagerFactory;
import org.xenei.jena.entities.MissingAnnotation;
import org.xenei.jena.entities.ResourceWrapper;

public class ColumnBuilder implements Column
{
	private ColumnDef columnDef;
	private Table table;
	private String name;
	private Class<? extends RdfColumn> typeClass = RdfColumn.class;
	private Class<? extends RdfTable> tableTypeClass = RdfTable.class;

	public ColumnBuilder()
	{
		
	}
	
	protected ColumnBuilder(Class<? extends RdfColumn> typeClass, Class<? extends RdfTable> tableTypeClass)
	{
		this.typeClass = typeClass;
		this.tableTypeClass = tableTypeClass;
	}
	
	public Column build( final Model model )
	{
		checkBuildState();

		final ResourceBuilder builder = new ResourceBuilder(model);

		Resource column = null;
		if (builder.hasResource(getFQName()))
		{
			column = builder.getResource(getFQName(), typeClass);
		}
		else
		{
			column = builder.getResource(getFQName(), typeClass);

			column.addLiteral(RDFS.label, name);
			column.addProperty(builder.getProperty(typeClass, "columnDef"),
					columnDef.getResource());

			column.addProperty(builder.getProperty(typeClass, "table"),
					table.getResource());

		}

		final EntityManager entityManager = EntityManagerFactory
				.getEntityManager();
		try
		{
			return entityManager.read(column, typeClass);
		}
		catch (final MissingAnnotation e)
		{
			throw new RuntimeException(e);
		}
	}

	protected void checkBuildState()
	{
		if (columnDef == null)
		{
			throw new IllegalStateException("columnDef must be set");
		}

		if (table == null)
		{
			throw new IllegalStateException("table must be set");
		}

		if (StringUtils.isBlank(getName()))
		{
			throw new IllegalStateException("Name must be set");
		}
	}

	@Override
	public Catalog getCatalog()
	{
		return table.getSchema().getCatalog();
	}

	@Override
	public ColumnDef getColumnDef()
	{
		return columnDef;
	}

	public String getFQName()
	{
		final StringBuilder sb = new StringBuilder()
				.append(getColumnDef().getResource().getURI()).append(" ")
				.append(name);
		return String.format("%s/instance/UUID-%s",
				ResourceBuilder.getFQName(RdfColumn.class),
				UUID.nameUUIDFromBytes(sb.toString().getBytes()));
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public Resource getResource()
	{
		return ResourceFactory.createResource(getFQName());
	}

	@Override
	public Schema getSchema()
	{
		return table.getSchema();
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
	public Table getTable()
	{
		return table;
	}

	public ColumnBuilder setColumnDef( final ColumnDef columnDef )
	{
		this.columnDef = columnDef;
		return this;
	}

	public ColumnBuilder setName( final String name )
	{
		this.name = name;
		return this;
	}

	public ColumnBuilder setTable( final Table table )
	{
		this.table = table;
		return this;
	}
}
