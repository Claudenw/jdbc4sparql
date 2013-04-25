package org.xenei.jdbc4sparql.config;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFList;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.J4SConnection;
import org.xenei.jdbc4sparql.iface.Catalog;
import org.xenei.jdbc4sparql.iface.ColumnDef;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.sparql.SparqlCatalog;
import org.xenei.jdbc4sparql.sparql.SparqlColumnDef;
import org.xenei.jdbc4sparql.sparql.SparqlSchema;
import org.xenei.jdbc4sparql.sparql.SparqlTable;
import org.xenei.jdbc4sparql.sparql.SparqlTableDef;

/**
 * A class to seriealize / deserialize configuration data.
 */
public class ConfigSerializer
{
	private static final String NS = "http://jdbc4sparql.xenei.org/entity/";
	private Model model;
	private Property catalogType;
	private Property catalogURL;
	private Property schemaType;
	private Property tableDefType;
	private Property tableDefQuerySegment;
	private Property columnDefLocalName;
	private Property columnDefNamespace;
	private Property columnDefType;
	private Property columnDefQuerySegment;
	private Property columnDefClassName;
	private Property columnDefDisplaySize;
	private Property columnDefNullable;
	private Property columnDefPrecision;
	private Property columnDefScale;
	private Property columnDefTypeType;
	private Property columnDefTypeName;
	private Property columnDefAutoIncrement;
	private Property columnDefCaseSensitive;
	private Property columnDefCurrency;
	private Property columnDefDefinitelyWritable;
	private Property columnDefReadOnly;
	private Property columnDefSearchable;
	private Property columnDefSigned;
	private Property columnDefWritable;

	/**
	 * Constructor.
	 */
	public ConfigSerializer()
	{
		model = ModelFactory.createDefaultModel();
		createProperties();
	}

	/**
	 * Add the contents of the J4SConnection configuration to the output.
	 * 
	 * @param connection
	 *            The J4SConnection to read configuration data from.
	 */
	public void add( final J4SConnection connection )
	{
		for (final Catalog catalog : connection.getCatalogs().values())
		{
			if (catalog instanceof SparqlCatalog)
			{
				add((SparqlCatalog) catalog);
			}
		}
	}

	private void add( final Resource tableDefR, final SparqlColumnDef columnDef )
	{
		Resource columnDefR = null;
		try
		{
			columnDefR = ResourceFactory.createResource(String.format(
					"http://jdbc4sparql.xenei.org/columnDef/UUID-%s",
					ColumnDef.Util.createID(columnDef)));
		}
		catch (final NoSuchAlgorithmException e)
		{
			throw new RuntimeException(e);
		}
		if (!model.contains(columnDefR, RDF.type, columnDefType))
		{
			columnDefR = model.createResource(columnDefR.getURI(),
					columnDefType);
			if (StringUtils.isNotEmpty(columnDef.getColumnClassName()))
			{
				columnDefR.addLiteral(columnDefClassName,
						columnDef.getColumnClassName());
			}
			columnDefR.addLiteral(columnDefDisplaySize,
					columnDef.getDisplaySize());
			if (StringUtils.isNotEmpty(columnDef.getLabel()))
			{
				columnDefR.addLiteral(RDFS.label, columnDef.getLabel());
			}
			columnDefR.addLiteral(columnDefNamespace, columnDef.getNamespace());
			columnDefR.addLiteral(columnDefLocalName, columnDef.getLocalName());
			columnDefR.addLiteral(columnDefNullable, columnDef.getNullable());
			columnDefR.addLiteral(columnDefPrecision, columnDef.getPrecision());
			columnDefR.addLiteral(columnDefScale, columnDef.getScale());
			columnDefR.addLiteral(columnDefTypeType, columnDef.getType());
			columnDefR.addLiteral(columnDefTypeName, columnDef.getTypeName());
			columnDefR.addLiteral(columnDefAutoIncrement,
					columnDef.isAutoIncrement());
			columnDefR.addLiteral(columnDefCaseSensitive,
					columnDef.isCaseSensitive());
			columnDefR.addLiteral(columnDefCurrency, columnDef.isCurrency());
			columnDefR.addLiteral(columnDefDefinitelyWritable,
					columnDef.isDefinitelyWritable());
			columnDefR.addLiteral(columnDefReadOnly, columnDef.isReadOnly());
			columnDefR
					.addLiteral(columnDefSearchable, columnDef.isSearchable());
			columnDefR.addLiteral(columnDefSigned, columnDef.isSigned());
			columnDefR.addLiteral(columnDefWritable, columnDef.isWritable());
			addQuerySegments(columnDefR, columnDefQuerySegment,
					columnDef.getQuerySegments());
		}
		else
		{
			columnDefR = model.createResource(columnDefR.getURI());
		}

		final Resource lst = tableDefR.getPropertyResourceValue(columnDefType);
		if (lst == null)
		{
			tableDefR.addProperty(columnDefType,
					model.createList().with(columnDefR));
		}
		else
		{
			lst.as(RDFList.class).add(columnDefR);
		}
	}

	private void add( final Resource catR, final SparqlSchema schema )
	{
		final Resource schemaR = model.createResource(schema.getFQName(),
				schemaType);
		schemaR.addLiteral(RDFS.label, schema.getLocalName());
		catR.addProperty(schemaType, schemaR);
		for (final Table table : schema.getTables())
		{
			if (table instanceof SparqlTable)
			{
				add(schemaR, (SparqlTable) table);
			}
		}
	}

	private void add( final Resource schemaR, final SparqlTable table )
	{
		final SparqlTableDef tableDef = table.getTableDef();
		Resource tableDefR = ResourceFactory.createResource(tableDef
				.getFQName());
		if (!model.contains(tableDefR, RDF.type, tableDefType))
		{
			tableDefR = model
					.createResource(tableDef.getFQName(), tableDefType);
			tableDefR.addLiteral(RDFS.label, table.getLocalName());
			for (final ColumnDef colDef : tableDef.getColumnDefs())
			{
				add(tableDefR, (SparqlColumnDef) colDef);
			}
			addQuerySegments(tableDefR, tableDefQuerySegment,
					tableDef.getQuerySegments());

		}
		else
		{
			tableDefR = model.createResource(tableDef.getFQName());
		}
		schemaR.addProperty(tableDefType, tableDefR);
	}

	/**
	 * Add an individual catalog to the configuration.
	 * 
	 * @param catalog
	 *            The SparqlCatalog to add to the configuration.
	 */
	public void add( final SparqlCatalog catalog )
	{
		final Resource catR = model.createResource(catalog.getFQName(),
				catalogType);
		catR.addLiteral(RDFS.label, catalog.getLocalName());
		if (catalog.isService())
		{
			catR.addLiteral(catalogURL, catalog.getServiceNode());
		}
		for (final Schema schema : catalog.getSchemas())
		{
			if (schema instanceof SparqlSchema)
			{
				add(catR, (SparqlSchema) schema);
			}
		}
	}

	private void addQuerySegments( final Resource r,
			final Property segmentProp, final List<String> segments )
	{
		r.removeAll(segmentProp);
		RDFList lst = null;
		if (segments.size() > 0)
		{
			lst = model.createList().with(
					model.createTypedLiteral(segments.get(0)));
			for (int i = 1; i < segments.size(); i++)
			{
				lst.add(model.createTypedLiteral(segments.get(i)));
			}
		}
		else
		{
			lst = model.createList();
		}
		r.addProperty(segmentProp, lst);
	}

	private void createProperties()
	{
		catalogType = model.createProperty(ConfigSerializer.NS, "Catalog");
		catalogURL = model.createProperty(ConfigSerializer.NS, "CatalogURL");

		schemaType = model.createProperty(ConfigSerializer.NS, "Schema");
		tableDefType = model.createProperty(ConfigSerializer.NS, "TableDef");
		tableDefQuerySegment = model.createProperty(ConfigSerializer.NS,
				"TableDefQuerySegment");
		columnDefType = model.createProperty(ConfigSerializer.NS, "ColumnDef");
		columnDefNamespace = model.createProperty(ConfigSerializer.NS,
				"ColumnDefNamespace");
		columnDefLocalName = model.createProperty(ConfigSerializer.NS,
				"ColumnDefLocalname");
		columnDefQuerySegment = model.createProperty(ConfigSerializer.NS,
				"ColumnDefQuerySegment");
		columnDefClassName = model.createProperty(ConfigSerializer.NS,
				"ColumnDefClassName");
		columnDefDisplaySize = model.createProperty(ConfigSerializer.NS,
				"ColumnDefDisplaySize");
		columnDefNullable = model.createProperty(ConfigSerializer.NS,
				"ColumnDefNullable");
		columnDefPrecision = model.createProperty(ConfigSerializer.NS,
				"ColumnDefPrecision");
		columnDefScale = model.createProperty(ConfigSerializer.NS,
				"ColumnDefScale");
		columnDefTypeType = model.createProperty(ConfigSerializer.NS,
				"ColumnDefType");
		columnDefTypeName = model.createProperty(ConfigSerializer.NS,
				"ColumnDefTypeName");
		columnDefAutoIncrement = model.createProperty(ConfigSerializer.NS,
				"ColumnDefAutoIncrement");
		columnDefCaseSensitive = model.createProperty(ConfigSerializer.NS,
				"ColumnDefCaseSensitive");
		columnDefCurrency = model.createProperty(ConfigSerializer.NS,
				"ColumnDefCurrency");
		columnDefDefinitelyWritable = model.createProperty(ConfigSerializer.NS,
				"ColumnDefDefinitelyWritable");
		columnDefReadOnly = model.createProperty(ConfigSerializer.NS,
				"ColumnDefReadOnly");
		columnDefSearchable = model.createProperty(ConfigSerializer.NS,
				"ColumnDefSearchable");
		columnDefSigned = model.createProperty(ConfigSerializer.NS,
				"ColumnDefSigned");
		columnDefWritable = model.createProperty(ConfigSerializer.NS,
				"ColumnDefWritable");
	}

	/**
	 * Deserialize the SparqlCatalog definition from the configuraiton.
	 * 
	 * If the catalog is not SPARQL type the local model data is not retrieved
	 * and must be
	 * repopulated using the ModelReager for the catalog.
	 * 
	 * @param catalogFQName
	 *            The fully qualified name of the catalog.
	 * @return The configured SparqlCatalog.
	 * @throws MalformedURLException
	 */
	public SparqlCatalog getCatalog( Dataset dataset,  final String catalogFQName )
			throws MalformedURLException
	{
		Resource catR = ResourceFactory.createResource(catalogFQName);
		if (model.contains(catR, RDF.type, catalogType))
		{
			SparqlCatalog retval = null;
			catR = model.createResource(catalogFQName, catalogType);
			final Resource catUrlR = catR.getPropertyResourceValue(catalogURL);
			if (catUrlR != null)
			{
				retval = new SparqlCatalog(new URL(catUrlR.getURI()),
						catR.getLocalName());
			}
			else
			{
				retval = new SparqlCatalog(catR.getNameSpace(),
						dataset.getNamedModel( catR.getLocalName() ), catR.getLocalName());
			}
			for (final Statement stmt : catR.listProperties(schemaType)
					.toList())
			{
				populateCatalog(retval, stmt.getResource());
			}
			return retval;
		}
		else
		{
			return null;
		}
	}

	/**
	 * Get a list of all Sparql catalogs in the configuration.
	 * 
	 * If a catalog is not SPARQL type the local model data is not retrieved and
	 * must be
	 * repopulated using the ModelReager for the catalog.
	 * 
	 * @return The list of catalogs.
	 * @param dataset the Dataset to add the catalog model to.
	 * @throws MalformedURLException
	 */
	public Collection<SparqlCatalog> getCatalogs(Dataset dataset) throws MalformedURLException
	{
		final List<SparqlCatalog> catalogs = new ArrayList<SparqlCatalog>();
		for (final Resource r : model.listResourcesWithProperty(RDF.type,
				catalogType).toList())
		{
			catalogs.add(getCatalog(dataset, r.getURI()));
		}
		return catalogs;
	}

	/**
	 * get a reader to read the saved configuraton file.
	 * 
	 * @return
	 */
	public ModelReader getLoader()
	{
		return new ModelReader() {

			@Override
			public Model getModel()
			{
				return ConfigSerializer.this.model;
			}
		};

	}

	private void populateCatalog( final SparqlCatalog catalog,
			final Resource schemaR )
	{
		final SparqlSchema schema = new SparqlSchema(catalog,
				schemaR.getNameSpace(), schemaR.getLocalName());
		for (final Statement stmt : schemaR.listProperties(tableDefType)
				.toList())
		{
			schema.addTableDef(retrieveTableDef(stmt.getResource()));
		}
		catalog.addSchema(schema);
	}

	private void populateTableDef( final SparqlTableDef tableDef,
			final Resource columnDefR )
	{
		final SparqlColumnDef.Builder builder = new SparqlColumnDef.Builder();

		for (final String querySegment : retrieveQuerySegments(columnDefR,
				columnDefQuerySegment))
		{
			builder.addQuerySegment(querySegment);
		}
		builder.setNamespace(
				columnDefR.getRequiredProperty(columnDefNamespace).getString())
				.setLocalName(
						columnDefR.getRequiredProperty(columnDefLocalName)
								.getString())
				.setType(
						columnDefR.getRequiredProperty(columnDefTypeType)
								.getInt())
				.setDisplaySize(
						columnDefR.getRequiredProperty(columnDefDisplaySize)
								.getInt())
				.setNullable(
						columnDefR.getRequiredProperty(columnDefNullable)
								.getInt())
				.setPrecision(
						columnDefR.getRequiredProperty(columnDefPrecision)
								.getInt())
				.setScale(
						columnDefR.getRequiredProperty(columnDefScale).getInt())
				.setTypeName(
						columnDefR.getRequiredProperty(columnDefTypeName)
								.getString())
				.setAutoIncrement(
						columnDefR.getRequiredProperty(columnDefAutoIncrement)
								.getBoolean())
				.setCaseSensitive(
						columnDefR.getRequiredProperty(columnDefCaseSensitive)
								.getBoolean())
				.setCurrency(
						columnDefR.getRequiredProperty(columnDefCurrency)
								.getBoolean())
				.setDefinitelyWritable(
						columnDefR.getRequiredProperty(
								columnDefDefinitelyWritable).getBoolean())
				.setReadOnly(
						columnDefR.getRequiredProperty(columnDefReadOnly)
								.getBoolean())
				.setSearchable(
						columnDefR.getRequiredProperty(columnDefSearchable)
								.getBoolean())
				.setSigned(
						columnDefR.getRequiredProperty(columnDefSigned)
								.getBoolean())
				.setWritable(
						columnDefR.getRequiredProperty(columnDefWritable)
								.getBoolean());

		Resource r = columnDefR.getPropertyResourceValue(columnDefClassName);
		if (r != null)
		{
			builder.setColumnClassName(r.asLiteral().getString());
		}
		r = columnDefR.getPropertyResourceValue(RDFS.label);
		if (r != null)
		{
			builder.setLabel(r.asLiteral().getString());
		}

		tableDef.add(builder.build());

	}

	private List<String> retrieveQuerySegments( final Resource r,
			final Property segmentProp )
	{
		final RDFList lst = r.getRequiredProperty(segmentProp).getResource()
				.as(RDFList.class);
		final List<String> retval = new ArrayList<String>();
		for (final RDFNode n : lst.asJavaList())
		{
			retval.add(n.asLiteral().getString());
		}
		return retval;
	}

	private SparqlTableDef retrieveTableDef( final Resource tableDefR )
	{
		SparqlTableDef tableDef = null;
		for (final String querySegment : retrieveQuerySegments(tableDefR,
				tableDefQuerySegment))
		{
			if (tableDef == null)
			{
				tableDef = new SparqlTableDef(tableDefR.getNameSpace(),
						tableDefR.getLocalName(), querySegment);
			}
			else
			{
				tableDef.addQuerySegment(querySegment);
			}
		}
		if (tableDef == null)
		{
			tableDef = new SparqlTableDef(tableDefR.getNameSpace(),
					tableDefR.getLocalName(), "");
		}
		final RDFList lst = tableDefR.getPropertyResourceValue(columnDefType)
				.as(RDFList.class);
		for (final RDFNode columnDefR : lst.asJavaList())
		{
			populateTableDef(tableDef, (Resource) columnDefR);
		}
		return tableDef;
	}

	public void save( final ModelWriter writer )
	{
		writer.write(model);
	}

}
