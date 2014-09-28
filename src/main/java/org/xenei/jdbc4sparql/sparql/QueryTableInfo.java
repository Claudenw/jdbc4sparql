package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.lang.sparql_11.ParseException;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.ColumnName;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TableName;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.sparql.parser.SparqlParser;

/**
 * Contains the information for the table in the query.  If a defined table is used multiple times
 * with different aliases there will be multiple QueryTableInfo instances with the same RdfTable but
 * different names.
 */
public class QueryTableInfo extends QueryItemInfo<TableName>
{
	/**
	 * 
	 */
	private final QueryInfoSet infoSet;
	private final Table<Column> table;
	private final ElementGroup eg;


	// list of type filters to add at the end of the query
	private final Set<CheckTypeF> typeFilterList;

	private static Logger LOG = LoggerFactory.getLogger(QueryTableInfo.class);
	
	public QueryTableInfo( final QueryInfoSet infoSet,
				final ElementGroup queryElementGroup, 
				final Table<Column> table,
				final TableName alias, final boolean optional )	
	{
		super( alias, optional );
		this.infoSet = infoSet;
		this.table = table;
		this.eg = new ElementGroup();
		this.typeFilterList = new HashSet<CheckTypeF>();
		infoSet.addTable(this);
		if (queryElementGroup == null)
		{
			QueryTableInfo.LOG.debug("marking {} as not in query.", this);
		}
		else {
		if (optional)
		{
			QueryTableInfo.LOG.debug("marking {} as optional.", this);
			queryElementGroup.addElement(new ElementOptional(eg));
		}
		else
		{
			QueryTableInfo.LOG.debug("marking {} as required.", this);
			queryElementGroup.addElement(eg);
		}
		}
	}
	
	/**
	 * Adds the required columns to the query.  Also adds the table definition.
	 * @param shortNames if true then short names will be used otherwise fully qualified names will be used.
	 * @param longNameColumns a list of columns that require long names.
	 */
	public void addRequiredColumns(boolean shortNames, List<String> longNameColumns) 
	{
		// add the table var to the nodes.
		QueryTableInfo.LOG.debug("adding required columns for {}", getName());
		final String eol = System.getProperty("line.separator");
		final StringBuilder queryFmt = new StringBuilder("{ ")
				.append(StringUtils.defaultString(table.getQuerySegmentFmt(),
						""));

		for (final Iterator<Column> colIter = table.getColumns(); colIter
				.hasNext();)
		{
			final Column column = colIter.next();
			boolean shortName = shortNames && ! longNameColumns.contains( column.getName() );
			if (!column.isOptional())
			{
				ColumnName name = column.getName();
				if (shortName)
				{
					name = new ColumnName(null, null, name.getShortName());
				}
				// make sure that it is in the queryInfo and not optional
				getColumn(name,false);
			}
		}
		
		// now add all the columns specified in the infoSet that are not optional
		ColumnName cName = null;
		if (infoSet.getShortNames())
		{
			cName = ColumnName.WILDNAME;
		}
		else
		{
			cName = getName().getColumnName(null);
		}
		for (QueryColumnInfo columnInfo : infoSet.listColumns( cName ))
		{
			if (!columnInfo.isOptional())
			{
				String fmt = columnInfo.getColumn().getQuerySegmentFmt();
				if (StringUtils.isNotBlank(fmt))
				{
					queryFmt.append(String.format(fmt, getVar(), columnInfo.getVar()))
							.append(eol);
				}
			}
		}
		queryFmt.append("}");
		final String queryStr = String.format(queryFmt.toString(), getVar());
		try
		{
			eg.addElement(SparqlParser.Util.parse(queryStr));
		}
		catch (final ParseException e)
		{
			throw new IllegalStateException(table.getName()
					+ " query segment " + queryStr, e);
		}
		catch (final QueryException e)
		{
			throw new IllegalStateException(table.getName()
					+ " query segment " + queryStr, e);
		}
		QueryTableInfo.LOG.debug("finished adding required columns for {}", getName());
	}

	/**
	 * Adds all of the table columns to the query.
	 * @param query The query to add the columns to
	 * 
	 */
	public void addTableColumns( Query query )
	{
		final Iterator<Column> iter = table.getColumns();
		while (iter.hasNext())
		{			
			Var v = Var.alloc(addColumnToQuery( iter.next(), infoSet.getShortNames() ));
			if (!query.getResultVars().contains(v.toString()))
			{
				query.addResultVar(v);
			}
		}
	}

	/**
	 * The 
	 * @param columnInfo
	 * @param cName
	 * @return
	 */
	public Node addColumnToQuery( QueryColumnInfo columnInfo, ColumnName cName)
	{
		return addColumnToQuery( columnInfo, cName, columnInfo.isOptional());
	}
	
	/**
	 * Adds the  column to the columns defined in the query.
	 * @param columnInfo The column to add 
	 * @param cName The name for the column
	 * @param optional if true the column will be optional the optional flag of the column info will be used.
	 * @Throws IllegalStateException if the columninfo is not in this table.
	 * @return The variable Node for the column.
	 */
	public Node addColumnToQuery( QueryColumnInfo columnInfo, ColumnName cName, boolean optional)
	{
		if (!infoSet.listColumns(getName().getColumnName(null)).contains( columnInfo ))
		{
			throw new IllegalStateException( String.format( SparqlQueryBuilder.NOT_FOUND_IN_, columnInfo.getName(), getName()));
		}
		return addColumnToQuery( columnInfo.getColumn(), cName, optional);
	}
	
	/**
	 * Adds the column to the columns defined in the query.
	 * If shortName is true then only the  column name is used as the name, otherwise
	 * the SQL name for the column is used.
	 * @param column The column to add.
	 * @param shortNames if True the short name is used.
	 * @return The variable Node for the column.
	 */
	public Node addColumnToQuery( Column column, boolean shortNames )
	{
		ColumnName cName = column.getName();
		if (shortNames)
		{
			cName = new ColumnName( null, null, cName.getShortName());
		}
		return addColumnToQuery( column, cName);
	}
	
	/**
	 * Adds the column to the columns defined in the query.  
	 * Result will be optional if the RdfColumn is optional.
	 * @param column The column to add
	 * @param cName The name for the column in the query.
	 * @return The variable Node for the column.
	 */
	public Node addColumnToQuery( Column column, ColumnName cName)
	{
		return addColumnToQuery( column, cName, column.isOptional());
	}
	
	/**
	 * Adds the column to the columns defined in the query.
	 * @param column The column to add
	 * @param cName The name for the column in the query
	 * @param optional if True the column is optional
	 * @return The variable Node for the column.
	 */
	public Node addColumnToQuery( Column column, ColumnName cName, boolean optional)
	{
		QueryColumnInfo columnInfo = new QueryColumnInfo( infoSet, this, column,
				cName, optional );
	
		QueryTableInfo.LOG.debug("Adding column: {} as {}", columnInfo,
				columnInfo.isOptional() ? "optional" : "required");

		if (column.hasQuerySegments() && columnInfo.isOptional())
		{
			eg.addElement(new ElementOptional( getQuerySegments(columnInfo.getColumn(), getVar(), columnInfo.getVar())));
		}

		typeFilterList.add(new CheckTypeF(columnInfo.getColumn(), 
				Var.alloc(columnInfo.getVar())));

		return columnInfo.getVar();
	}
	
	private Element getQuerySegments( final Column column, final Node tableVar, final Node columnVar )
	{
		final String fmt = "{" + column.getQuerySegmentFmt() + "}";

		try
		{
			return SparqlParser.Util.parse(String.format(fmt, tableVar,
					columnVar));
		}
		catch (final ParseException e)
		{
			throw new IllegalStateException(column.getName() + " query segment "
					+ fmt, e);
		}
		catch (final QueryException e)
		{
			throw new IllegalStateException(column.getName() + " query segment "
					+ fmt, e);
		}
	}

	public void addFilter( final Expr expr )
	{
		QueryTableInfo.LOG.debug("Adding filter: {}", expr);
		eg.addElementFilter(new ElementFilter(expr));
	}
	
	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo property.
	 * Optional state is determined from the optional state of the tableColumnInfo.
	 * @param tableColumnInfo The column to alias
	 * @param alias The name of the alias column
	 */
	public void setEquals( QueryColumnInfo tableColumnInfo, ColumnName aliasName)
	{
		QueryColumnInfo ci = infoSet.getColumnByName( aliasName );
		if (ci != null)
		{
			setEquals( tableColumnInfo, ci);
		}
		else
		{
			throw new IllegalArgumentException( String.format( "%s is not a Query Column", aliasName));
		}
		
	}
	
	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo property.
	 * Optional state is determined from the optional state of the tableColumnInfo.
	 * @param tableColumnInfo The column to alias
	 * @param aliasTableInfo The alias column
	 */
	public void setEquals( QueryColumnInfo tableColumnInfo, QueryColumnInfo aliasTableInfo )
	{
		setEquals( tableColumnInfo, aliasTableInfo, tableColumnInfo.isOptional());
	}
	
	/**
	 * Adds the variable for aliasTableInfo as the value of the tableColumnInfo property.
	 * @param tableColumnInfo The column to alias
	 * @param aliasColumnInfo The alias column
	 * @param optional determines if the entry should be optional.
	 */
	public void setEquals( QueryColumnInfo tableColumnInfo, QueryColumnInfo aliasColumnInfo, boolean optional )
	{
		if (!infoSet.listColumns(getName().getColumnName(null)).contains( tableColumnInfo ))
		{
			throw new IllegalStateException( String.format( SparqlQueryBuilder.NOT_FOUND_IN_, tableColumnInfo.getName(), getName()));
		}	
		if ( ! tableColumnInfo.getColumn().hasQuerySegments())
		{
			throw new IllegalArgumentException( String.format( "%s may not be aliased", tableColumnInfo.getName()));
		}
		if (optional)
		{
			eg.addElement(new ElementOptional(getQuerySegments(tableColumnInfo.getColumn(),
					getVar(), aliasColumnInfo.getVar())));
		}
		else
		{
			eg.addElement(new ElementOptional(getQuerySegments(tableColumnInfo.getColumn(),
					getVar(), aliasColumnInfo.getVar())));
		}	
	}

	public void addTypeFilters()
	{
		for (final CheckTypeF f : typeFilterList)
		{
			addFilter(f);
		}
	}

	/**
	 * Returns the column or null if not found
	 * 
	 * @param name
	 *            The name of the column to look for.
	 * @return QueryColumnInfo for the column
	 */
	public QueryColumnInfo getColumn( ColumnName cName )
	{
		return getColumn( cName, true );
	}
	
	/**
	 * Returns the column or null if not found
	 * 
	 * @param name
	 *            The name of the column to look for.
	 * @param optional
	 * 			If false then column is required, if true then the column isOptional flag defines value
	 * @return
	 * @throws SQLException 
	 */
	public QueryColumnInfo getColumn( ColumnName cName, boolean optional ) 
	{
		if ( ! (cName.getTableName().matches(table.getName())))
		{
			return null;
		}
		QueryColumnInfo retval = infoSet.findColumn(cName);
		if (retval == null)
		{
			final Column col = table.getColumn(cName.getCol());
			if (col != null)
			{
				boolean opt = optional?col.isOptional():SparqlQueryBuilder.REQUIRED;
				addColumnToQuery( col, table.getName().getColumnName(cName.getCol()), opt );
				retval = infoSet.findColumn( cName );
			}	
		}
		if (retval != null && retval.getName().getTableName().matches(table.getName()))
		{
			if (retval.isOptional() && !optional)
			{
				((QueryColumnInfo)retval).setOptional( false );
			}
			return retval;
		}
		return null;
	}

	public Table<Column> getTable()
	{
		return table;
	}
	
	public Collection<QueryColumnInfo> getColumns()
	{		
		return infoSet.listColumns( ColumnName.getNameInstance( getName()));
	}

	public String getSQLName()
	{
		return getName().getDBName();
	}

	public Set<CheckTypeF> getTypeFilterList()
	{
		return typeFilterList;
	}

	@Override
	public String toString()
	{
		return String.format("QueryTableInfo[%s(%s)]", table.getSQLName(),
				getName());
	}
}