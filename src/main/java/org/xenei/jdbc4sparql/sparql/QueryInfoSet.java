package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;

/**
 * A collection of all tables and columns in the query.
 * The column names are index by full aliased table name based names (no short names)
 */
public class QueryInfoSet
{

	// the list of tables in the query indexed by SQL name.
	private final Map<QueryTableInfo.Name, QueryTableInfo> tablesInQuery;

	// the list of columns in the query indexed by SQL name.
	private final Map<QueryColumnInfo.Name, QueryColumnInfo> columnsInQuery;

	private static final Logger LOG = LoggerFactory
			.getLogger(QueryInfoSet.class);
	
	private boolean shortNames;

	public QueryInfoSet()
	{
		this.tablesInQuery = new LinkedHashMap<QueryTableInfo.Name, QueryTableInfo>();
		this.columnsInQuery = new LinkedHashMap<QueryColumnInfo.Name, QueryColumnInfo>();
		this.shortNames = true;
	}
	
	public void addRequiredColumns( List<String> longNameColumns ) 
	{
		if (tablesInQuery.isEmpty())
		{
			throw new IllegalArgumentException( "Must have at least one table");
		}
		for (QueryTableInfo tableInfo : tablesInQuery.values())
		{
			tableInfo.addRequiredColumns(shortNames, longNameColumns);
		}
	}

	public void addColumn( final QueryTableInfo tableInfo, final QueryColumnInfo columnInfo)
	{
		if (tableInfo == null)
		{
			throw new IllegalArgumentException( "tableInfo may not be null");
		}
		QueryColumnInfo.Name cName = null;
		// check for short names
		if (shortNames) {
			cName = QueryColumnInfo.getNameInstance(columnInfo.getName().getCol());
		}
		else {
			cName = tableInfo.getName().getColumnName(columnInfo.getName().getCol());
		}
		QueryInfoSet.LOG.debug("adding column: {} as {}", columnInfo, cName);
		columnsInQuery.put(cName, columnInfo);
	}

	public void addTable( final QueryTableInfo tbl )
	{
		QueryInfoSet.LOG.debug("adding table: {}", tbl);
		tablesInQuery.put(tbl.getName(), tbl);
		shortNames = tablesInQuery.size() == 1;
	}
	
	public void setShortNames( boolean state )
	{
		this.shortNames = state;
	}
	
	public boolean getShortNames()
	{
		return shortNames;
	}

	/**
	 * Returns true if the name matches any of the column names.
	 * @see {QueryItemName.findMatch()}
	 * @param name The name to match
	 * @return true if the name matches is found.
	 */
	public boolean containsColumn( final String columnName )
	{
		return containsColumn( QueryColumnInfo.getNameInstance(columnName) );
	}
	
	/**
	 * Returns true if the name matches any of the column names.
	 * @see {QueryItemName.findMatch()}
	 * @param name The name to match
	 * @return true if the name matches is found.
	 */
	public boolean containsColumn( QueryItemName name)
	{
		return name.findMatch(columnsInQuery) != null;
	}

	
	/**
	 * Find the column in the query.  Returns null if not found.
	 * @param schemaName
	 * @param tableName
	 * @param columnName
	 * @return columnInfo for column or null if not found.
	 */
	public QueryColumnInfo findColumn( QueryColumnInfo.Name name )
	{
		return name.findMatch(columnsInQuery);
	}
	
	/**
	 * Returns the column from the query if it exists.  Otherwise scan across the tables in the query looking
	 * for the column.  If found add it to the query and return it. otherwise return null.
	 * @param name
	 * @return
	 */
	public QueryColumnInfo scanTablesForColumn( QueryColumnInfo.Name cName ) 
	{
		// check exact match
		QueryColumnInfo retval = findColumn( cName );
		if (retval == null)
		{		
			// table name may be wild card
			QueryTableInfo.Name tName = QueryTableInfo.getNameInstance(cName);
			QueryTableInfo tableInfo = null;
			for (QueryTableInfo testTableInfo : listTables(tName))
			{
				if (testTableInfo.getColumn( cName ) != null)
				{
					if (tableInfo != null)
					{
						LOG.info( String.format(
								SparqlQueryBuilder.FOUND_IN_MULTIPLE_, cName, "tables") );
						return null;
					}
					tableInfo = testTableInfo;
				}
			}
			
			if (tableInfo != null)
			{
				retval = tableInfo.getColumn(cName);
			}
			
		}
		return retval;
	}

	/**
	 * Retrieves the column form the list of query columns
	 * @param name The name to retrieve
	 * @return The column info for the named column
	 * @throws IllegalStateException if the column is not found
	 */
	public QueryColumnInfo getColumnByName( final String name )
	{
		return getColumnByName( QueryColumnInfo.getNameInstance(name) );
	}

	/**
	 * Retrieves the column form the list of query columns
	 * @param name The name to retrieve
	 * @return The column info for the named column
	 * @throws IllegalStateException if the column is not found
	 */
	public QueryColumnInfo getColumnByName( final Var name )
	{
		return getColumnByName( QueryColumnInfo.getNameInstance( NameUtils.convertSPARQL2DB(name.getVarName())) );
	}
	/**
	 * Retrieves the column form the list of query columns
	 * @param name The name to retrieve
	 * @return The column info for the named column
	 * @throws IllegalStateException if the column is not found
	 */
	public QueryColumnInfo getColumnByName( final QueryColumnInfo.Name name )
	{
		final QueryColumnInfo retval = name.findMatch(columnsInQuery);
		if (retval == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, name));
		}
		return retval;
	}
	
	
	public int getColumnIndex( final QueryColumnInfo.Name name )
	{
		int i = 0;
		for (QueryColumnInfo.Name cName : columnsInQuery.keySet())
		{
			if (name.equals( cName ))
			{
				return i;
			}
			i++;
		}
		return -1;
	}
	
	/**
	 * Retrieves the column form the list of query columns
	 * @param node The node to retrieve
	 * @return The column info for the column with that node
	 * @throws IllegalStateException if the column is not found
	 */
	public QueryColumnInfo getColumnByNode( final Node node )
	{
		return getItemByNode(columnsInQuery, node);
	}

	public List<QueryColumnInfo> getColumns()
	{
		return WrappedIterator.create(columnsInQuery.values().iterator()).toList();
	}

	private <T extends QueryItemInfo<?>> T getItemByNode(
			final Map<? extends QueryItemName, T> map, final Node n )
	{
		for (final T entry : map.values())
		{
			if (entry.getVar().equals(n))
			{
				return entry;
			}
		}
		return null;
	}

	/**
	 * Get the table info from the tables in the query
	 * @param name The table name to find.
	 * @return The query table info for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public QueryTableInfo getTable( final QueryTableInfo.Name name )
	{
		return name.findMatch(tablesInQuery);
	}

	/**
	 * Get the table info from the tables in the quer
	 * @param name The table name to find. 
	 * @return The query table info for the name or null if none found
	 * @throws IllegalArgumentException
	 *             if more than one object matches.
	 */
	public QueryTableInfo getTable( final String name )
	{
		return getTable(QueryTableInfo.getNameInstance(name));
	}

	public Set<String> getTableAliases()
	{
		return WrappedIterator.create(tablesInQuery.keySet().iterator())
				.mapWith(new Map1<QueryTableInfo.Name, String>() {

					@Override
					public String map1( final QueryTableInfo.Name o )
					{
						return o.toString();
					}
				}).toSet();
	}

	public QueryTableInfo getTableByNode( final Node n )
	{
		return getItemByNode(tablesInQuery, n);
	}

	public Set<String> getTableNames()
	{
		return WrappedIterator.create(tablesInQuery.values().iterator())
				.mapWith(new Map1<QueryTableInfo, String>() {

					@Override
					public String map1( final QueryTableInfo o )
					{
						return o.getSQLName();
					}
				}).toSet();
	}

	public Collection<QueryTableInfo> getTables()
	{
		return tablesInQuery.values();
	}

	public Collection<QueryTableInfo> listTables()
	{
		return tablesInQuery.values();
	}
	
	public Collection<QueryTableInfo> listTables(QueryTableInfo.Name name)
	{
		return name.listMatches(tablesInQuery);
	}
	
	/**
	 * List the columns that match the name.
	 * @param name The Column name to match
	 * @return The list o
	 */
	public Collection<QueryColumnInfo> listColumns( QueryColumnInfo.Name name )
	{
		return name.listMatches(columnsInQuery);
	}
}
