/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xenei.jdbc4sparql.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.PathBlock;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.function.FunctionEnv;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementAssign;
import com.hp.hpl.jena.sparql.syntax.ElementBind;
import com.hp.hpl.jena.sparql.syntax.ElementData;
import com.hp.hpl.jena.sparql.syntax.ElementDataset;
import com.hp.hpl.jena.sparql.syntax.ElementExists;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementMinus;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementNotExists;
import com.hp.hpl.jena.sparql.syntax.ElementOptional;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementService;
import com.hp.hpl.jena.sparql.syntax.ElementSubQuery;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.ElementUnion;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.WrappedIterator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang3.StringUtils;
import org.xenei.jdbc4sparql.iface.Column;
import org.xenei.jdbc4sparql.iface.Schema;
import org.xenei.jdbc4sparql.iface.Table;
import org.xenei.jdbc4sparql.iface.TypeConverter;
import org.xenei.jdbc4sparql.impl.AbstractResultSet;
import org.xenei.jdbc4sparql.impl.NameUtils;
import org.xenei.jdbc4sparql.impl.rdf.RdfCatalog;
import org.xenei.jdbc4sparql.impl.rdf.RdfColumn;
import org.xenei.jdbc4sparql.impl.rdf.RdfKey;
import org.xenei.jdbc4sparql.impl.rdf.RdfKeySegment;
import org.xenei.jdbc4sparql.impl.rdf.RdfTable;
import org.xenei.jdbc4sparql.impl.rdf.RdfTableDef;

/**
 * Creates a SparqlQuery while tracking naming changes between nomenclatures.
 */
public class SparqlQueryBuilder
{

	/**
	 * Class to renumber bNodes to be unique within the query.
	 */
	private static class BnodeRenumber
	{
		private class ElementHandler implements ElementVisitor
		{
			EType type;

			public EType getType()
			{
				return type;
			}

			@Override
			public void visit( final ElementAssign el )
			{
				type = EType.Assign;
			}

			@Override
			public void visit( final ElementBind el )
			{
				type = EType.Bind;
			}

			@Override
			public void visit( final ElementData el )
			{
				type = EType.Data;
			}

			@Override
			public void visit( final ElementDataset el )
			{
				type = EType.Dataset;
			}

			@Override
			public void visit( final ElementExists el )
			{
				type = EType.Exists;
			}

			@Override
			public void visit( final ElementFilter el )
			{
				type = EType.Filter;
			}

			@Override
			public void visit( final ElementGroup el )
			{
				type = EType.Group;
			}

			@Override
			public void visit( final ElementMinus el )
			{
				type = EType.Minus;
			}

			@Override
			public void visit( final ElementNamedGraph el )
			{
				type = EType.NamedGraph;
			}

			@Override
			public void visit( final ElementNotExists el )
			{
				type = EType.NotExists;
			}

			@Override
			public void visit( final ElementOptional el )
			{
				type = EType.Optional;
			}

			@Override
			public void visit( final ElementPathBlock el )
			{
				type = EType.PathBlock;
			}

			@Override
			public void visit( final ElementService el )
			{
				type = EType.Service;
			}

			@Override
			public void visit( final ElementSubQuery el )
			{
				type = EType.SubQuery;
			}

			@Override
			public void visit( final ElementTriplesBlock el )
			{
				type = EType.TriplesBlock;
			}

			@Override
			public void visit( final ElementUnion el )
			{
				type = EType.Union;
			}
		}

		enum EType
		{
			Assign, Bind, Data, Dataset, Exists, Filter, Group, Minus, NamedGraph, NotExists, Optional, PathBlock, Service, SubQuery, TriplesBlock, Union
		}

		private int bnodeCount = 0;
		private final ElementHandler handler = new ElementHandler();;
		private Map<String, Node> renumberMap = new HashMap<String, Node>();

		private final Stack<Map<String, Node>> renumberMapStack = new Stack<Map<String, Node>>();

		private Node nextAnon()
		{
			final AnonId id = new AnonId("?" + bnodeCount);
			bnodeCount++;
			return NodeFactory.createAnon(id);
		}

		private Expr processExpr( final Expr expr )
		{
			if (expr.isVariable())
			{
				final ExprVar ev = expr.getExprVar();
				if (ev.asVar().isBlankNodeVar())
				{
					return new ExprVar(nextAnon());
				}
			}
			return expr;
		}

		private Node processNode( Node n )
		{
			if (n.isBlank())
			{
				Node retval = renumberMap.get(n.getBlankNodeLabel());
				if (retval == null)
				{
					retval = nextAnon();
					renumberMap.put(n.getBlankNodeLabel(), retval);
				}
				return retval;
			}
			if (n.isVariable())
			{
				n = processVar((Var) n);
			}
			return n;
		}

		private Var processVar( final Var var )
		{
			Var v = var;
			if (v.isBlankNodeVar())
			{
				final String s = v.getVarName();
				final AnonId id = new AnonId(s);
				Node n = renumberMap.get(id.getLabelString());
				if (n == null)
				{
					n = nextAnon();

					renumberMap.put(id.getLabelString(), n);
				}
				v = Var.alloc(n.getBlankNodeId().getLabelString());
			}
			return v;
		}

		public Element renumber( final Element e )
		{
			Element retval = e;
			e.visit(handler);
			switch (handler.getType())
			{
				case Assign:
					retval = renumberAssign((ElementAssign) e);
					break;
				case Bind:
					retval = renumberBind((ElementBind) e);
					break;
				case Data:
					retval = renumberData((ElementData) e);
					break;
				case Dataset:
					throw new IllegalArgumentException(
							"Dataset should not be used in parser");

				case Exists:
					retval = new ElementExists(
							renumber(((ElementExists) e).getElement()));
					break;
				case Filter:
					retval = new ElementFilter(
							processExpr(((ElementFilter) e).getExpr()));
					break;
				case Group:
					retval = renumberGroup((ElementGroup) e);
					break;
				case Minus:
					retval = new ElementMinus(
							renumber(((ElementMinus) e).getMinusElement()));
					break;
				case NamedGraph:
					retval = renumberNamedGraph((ElementNamedGraph) e);
					break;
				case NotExists:
					retval = new ElementNotExists(
							renumber(((ElementNotExists) e).getElement()));
					break;
				case Optional:
					retval = new ElementOptional(
							renumber(((ElementOptional) e).getOptionalElement()));
					break;
				case PathBlock:
					retval = renumberPathBlock((ElementPathBlock) e);
					break;
				case Service:
					// default to returning e
					break;
				case SubQuery:
					// default to returning e
					break;
				case TriplesBlock:
					retval = renumberTriplesBlock((ElementTriplesBlock) e);
					break;
				case Union:
					retval = renumberUnion((ElementUnion) e);
					break;
			}
			return retval;

		}

		private ElementAssign renumberAssign( final ElementAssign e )
		{
			return new ElementAssign(processVar(e.getVar()),
					processExpr(e.getExpr()));
		}

		private ElementBind renumberBind( final ElementBind e )
		{
			return new ElementBind(processVar(e.getVar()),
					processExpr(e.getExpr()));
		}

		private ElementData renumberData( final ElementData e )
		{
			final List<Var> vars = e.getVars();
			boolean foundBlank = false;
			for (int i = 0; i < vars.size(); i++)
			{
				if (vars.get(i).isBlankNodeVar())
				{
					foundBlank = true;
					vars.set(i, processVar(vars.get(i)));
				}
			}
			ElementData retval = e;
			if (foundBlank)
			{
				retval = new ElementData();
				for (final Var v : vars)
				{
					retval.add(v);
				}
			}
			return retval;
		}

		private ElementGroup renumberGroup( final ElementGroup e )
		{
			renumberMapStack.push(renumberMap);
			renumberMap = new HashMap<String, Node>();
			final ElementGroup retval = new ElementGroup();
			for (final Element el : e.getElements())
			{
				retval.addElement(renumber(el));
			}
			renumberMap = renumberMapStack.pop();
			return retval;
		}

		private ElementNamedGraph renumberNamedGraph( final ElementNamedGraph e )
		{
			final Node n = e.getGraphNameNode();
			if (n != null)
			{
				return new ElementNamedGraph(processNode(n),
						renumber(e.getElement()));
			}
			else
			{
				return new ElementNamedGraph(renumber(e.getElement()));
			}
		}

		private ElementPathBlock renumberPathBlock( final ElementPathBlock e )
		{
			final ElementPathBlock retval = new ElementPathBlock();
			final PathBlock pb = e.getPattern();
			for (final TriplePath tp : pb.getList())
			{
				if (tp.isTriple())
				{
					final Triple t = new Triple(processNode(tp.getSubject()),
							tp.getPredicate(), processNode(tp.getObject()));
					retval.addTriple(t);
				}
				else
				{
					final TriplePath tp2 = new TriplePath(
							processNode(tp.getSubject()), tp.getPath(),
							processNode(tp.getObject()));
					retval.addTriple(tp2);
				}
			}
			return retval;
		}

		private ElementTriplesBlock renumberTriplesBlock(
				final ElementTriplesBlock e )
		{
			final ElementTriplesBlock retval = new ElementTriplesBlock();
			final Iterator<Triple> iter = e.patternElts();
			while (iter.hasNext())
			{
				final Triple tp = iter.next();
				retval.addTriple(new Triple(processNode(tp.getSubject()), tp
						.getPredicate(), processNode(tp.getObject())));
			}
			return retval;
		}

		private ElementUnion renumberUnion( final ElementUnion e )
		{
			final ElementUnion retval = new ElementUnion();
			for (final Element el : e.getElements())
			{
				retval.addElement(renumber(el));
			}
			return retval;
		}
	}

	/**
	 * A local filter that removes any values that are null and not allowed to
	 * be null or that can not be converted
	 * to the expected column value type.
	 */
	public class CheckTypeF extends ExprFunction1
	{
		private final RdfColumn column;

		public CheckTypeF( final RdfColumn column, final Node columnVar )
		{
			super(new ExprVar(columnVar), "checkTypeF");
			this.column = column;
		}

		@Override
		public Expr copy( final Expr expr )
		{
			return new CheckTypeF(column, expr.asVar());
		}

		@Override
		public boolean equals( final Object o )
		{
			if (o instanceof CheckTypeF)
			{
				final CheckTypeF cf = (CheckTypeF) o;
				return column.equals(cf.column)
						&& getExpr().asVar().equals(cf.getExpr().asVar());
			}
			return false;
		}

		@Override
		public NodeValue eval( final NodeValue v )
		{
			return NodeValue.FALSE;
		}

		@Override
		protected NodeValue evalSpecial( final Binding binding,
				final FunctionEnv env )
		{
			final Var v = Var.alloc(column.getSPARQLName());
			final Node n = binding.get(v);
			if (n == null)
			{
				return column.getColumnDef().getNullable() == ResultSetMetaData.columnNullable ? NodeValue.TRUE
						: NodeValue.FALSE;
			}
			final Class<?> resultingClass = TypeConverter.getJavaType(column
					.getColumnDef().getType());
			Object columnObject;
			if (n.isLiteral())
			{
				columnObject = n.getLiteralValue();
			}
			else if (n.isURI())
			{
				columnObject = n.getURI();
			}
			else if (n.isBlank())
			{
				columnObject = n.getBlankNodeId();
			}
			else if (n.isVariable())
			{
				columnObject = n.getName();
			}
			else
			{
				columnObject = n.toString();
			}

			try
			{
				AbstractResultSet.extractData(columnObject, resultingClass);
				return NodeValue.TRUE;
			}
			catch (final SQLException e)
			{
				return NodeValue.FALSE;
			}

		}
	}

	public class RdfTableInfo
	{
		private final RdfTable table;
		// private final ElementTriplesBlock etb;
		private final ElementGroup eg;
		private final Node tableVar;
		private final boolean optional;
		// list of type filters to add at the end of the query
		private final Set<CheckTypeF> typeFilterList;

		private RdfTableInfo( final ElementGroup queryElementGroup,
				final RdfTable table, final boolean optional )
		{
			this.table = table;
			this.eg = new ElementGroup();
			// this.etb = new ElementTriplesBlock();
			this.optional = optional;
			this.typeFilterList = new HashSet<CheckTypeF>();
			// eg.addElement(etb);

			tablesInQuery.put(table.getSQLName(), this);
			// add the table var to the nodes.
			tableVar = NodeFactory.createVariable(table.getSPARQLName());
			nodesInQuery.put(table.getSQLName(), tableVar);
			final Element el = table.getQuerySegments(tableVar);
			if (el != null)
			{
				eg.addElement(el);
			}
			// add all the required columns
			for (final Iterator<RdfColumn> colIter = table.getColumns(); colIter
					.hasNext();)
			{
				final RdfColumn column = colIter.next();
				if (!column.isOptional())
				{
					addColumn(column);
				}
			}

			if (optional)
			{
				queryElementGroup.addElement(new ElementOptional(eg));
			}
			else
			{
				queryElementGroup.addElement(eg);
			}
		}

		/**
		 * Add the column to the columns being returned from this table.
		 * 
		 * @param column
		 *            the column to return
		 * @return The columnVar node.
		 * @Throws IllegalArgumentException if column is not from this table.
		 */
		public Node addColumn( final RdfColumn column )
		{
			if (!column.getTable().equals(table))
			{
				throw new IllegalArgumentException(String.format(
						"Column %s may not be added to table %s",
						column.getSQLName(), table.getSQLName()));
			}
			// add the column to the query.
			final Node columnVar = NodeFactory.createVariable(column.getSPARQLName());
			// if the column does not allow null add triple

			if (!columnsInQuery.containsKey(column.getSQLName()))
			{
				columnsInQuery.put(column.getSQLName(), column);
				nodesInQuery.put(column.getSQLName(), columnVar);
			}
			final Element e = column.getQuerySegments(tableVar, columnVar);
			if (column.isOptional())
			{
				eg.addElement(new ElementOptional(e));
			}
			else
			{
				eg.addElement(e);
			}

			typeFilterList.add(new CheckTypeF(column, columnVar));

			return columnVar;
		}

		public void addFilter( final Expr expr )
		{
			eg.addElementFilter(new ElementFilter(expr));
		}

		public void addOptional( final ElementTriplesBlock etb )
		{
			eg.addElement(new ElementOptional(etb));
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
		 * @return
		 */
		public RdfColumn getColumn( final String name )
		{
			return (RdfColumn) table.getColumn(name);
		}

		public Iterator<RdfColumn> getColumns()
		{
			return table.getColumns();
		}

		public String getSQLName()
		{
			return table.getSQLName();
		}

		public Node getTableVar()
		{
			return tableVar;
		}

		public Set<CheckTypeF> getTypeFilterList()
		{
			return typeFilterList;
		}

		public boolean isOptional()
		{
			return optional;
		}
	}

	// a set of error messages.
	private static final String NOT_FOUND_IN_QUERY = "%s not found in SPARQL query";

	private static final String FOUND_IN_MULTIPLE_ = "%s was found in multiple %s";

	private static final String NOT_FOUND_IN_ANY_ = "%s was not found in any %s";

	private static void addFilter( final Query query, final Expr filter )
	{
		final ElementFilter el = new ElementFilter(filter);
		SparqlQueryBuilder.getElementGroup(query).addElementFilter(el);
	}

	private static ElementGroup getElementGroup( final Query query )
	{
		ElementGroup retval;
		final Element e = query.getQueryPattern();
		if (e == null)
		{
			retval = new ElementGroup();
			query.setQueryPattern(retval);
		}
		else if (e instanceof ElementGroup)
		{
			retval = (ElementGroup) e;
		}
		else
		{
			retval = new ElementGroup();
			retval.addElement(e);
		}
		return retval;
	}

	// the query we are building.
	private Query query;

	// query was built flag;
	private boolean isBuilt;

	// sparql catalog we are running against.
	private final RdfCatalog catalog;

	// the list of tables in the query indexed by SQL name.
	private final Map<String, RdfTableInfo> tablesInQuery;

	// the list of nodes in the query indexed by SQL name.
	private final Map<String, Node> nodesInQuery;

	// the list of columns in the query indexed by SQL name.
	private final Map<String, RdfColumn> columnsInQuery;

	// the list of columns not to be included in the "all columns" result.
	// perhaps this should store column so that tables may be checked in case
	// tables are added to the query later. But I don't think so.
	private final List<String> columnsInUsing;

	// columns indexed by var.
	private final List<Column> columnsInResult;

	/**
	 * Create a query builder for a catalog
	 * 
	 * @param catalog
	 *            The catalog to build the query for.
	 * @throws IllegalArgumentException
	 *             if catalog is null.
	 */
	public SparqlQueryBuilder( final RdfCatalog catalog )
	{
		if (catalog == null)
		{
			throw new IllegalArgumentException("Catalog may not be null");
		}
		this.catalog = catalog;
		this.query = new Query();
		this.isBuilt = false;
		this.tablesInQuery = new HashMap<String, RdfTableInfo>();
		this.nodesInQuery = new HashMap<String, Node>();
		this.columnsInQuery = new HashMap<String, RdfColumn>();
		this.columnsInUsing = new ArrayList<String>();
		this.columnsInResult = new ArrayList<Column>();
		query.setQuerySelectType();
	}

	/**
	 * Add the triple to the BGP for the query.
	 * This method handles adding the triple to the proper section of the
	 * query.
	 * 
	 * @param t
	 *            The Triple to add.
	 */
	public void addBGP( final Triple t )
	{
		checkBuilt();
		final ElementGroup eg = getElementGroup();
		// final Triple t = new Triple(s, p, o);
		for (final Element el : eg.getElements())
		{
			if (el instanceof ElementTriplesBlock)
			{
				if (((ElementTriplesBlock) el).getPattern().getList()
						.contains(t))
				{
					return;
				}
			}
		}
		eg.addTriplePattern(t);
	}

	/**
	 * Add a column to the query.
	 * 
	 * Adds the column to the query tracking the name changes as necessary.
	 * If the column has not already been added to the query this method does
	 * the following
	 * <ul>
	 * <li>
	 * Adds the column table to the table list if not already added.</li>
	 * <li>
	 * Adds the column to the bgp using the column query segments.</li>
	 * <li>
	 * Makes the column values SPARQL optional if they are SQL nullable.</li>
	 * </ul>
	 * 
	 * @param column
	 *            The column to add
	 * @return The SPARQL based node name for the column.
	 * @throws SQLException
	 */
	public Node addColumn( final RdfColumn column ) throws SQLException
	{
		checkBuilt();
		if (!columnsInQuery.containsKey(column.getSQLName()))
		{
			RdfTableInfo sti = tablesInQuery
					.get(column.getTable().getSQLName());
			if (sti == null)
			{
				sti = new RdfTableInfo(getElementGroup(), column.getTable(),
						false);
			}
			return sti.addColumn(column);
		}
		else
		{
			// already there, return node from the query.
			return getNodeBySQLName(column.getSQLName());
		}
	}

	/**
	 * Add teh column to the query.
	 * 
	 * @param schemaName
	 *            The schema name.
	 * @param tableName
	 *            The table name.
	 * @param columnName
	 *            The column name.
	 * @return The node for the column.
	 * @throws SQLException
	 */
	public Node addColumn( final String schemaName, final String tableName,
			final String columnName ) throws SQLException
	{
		checkBuilt();
		// get all the columns that match the name patterns
		final Collection<RdfColumn> columns = findColumns(schemaName,
				tableName, columnName);

		// if there are more than one see if only one is specified in the list
		// of tables.
		if (columns.size() > 1)
		{
			if (tableName == null)
			{
				// wild table so look for table names currently in the query.
				// get the set of table names.
				final Set<String> tblNames = WrappedIterator
						.create(tablesInQuery.values().iterator())
						.mapWith(new Map1<RdfTableInfo, String>() {

							@Override
							public String map1( final RdfTableInfo o )
							{
								return o.getSQLName();
							}
						}).toSet();
				RdfColumn col = null;
				RdfColumn thisCol = null;
				final Iterator<RdfColumn> iter = columns.iterator();
				while (iter.hasNext())
				{
					thisCol = iter.next();
					if (tblNames.contains(thisCol.getTable().getSQLName()))
					{
						if (col != null)
						{
							throw new SQLException(String.format(
									SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
									columnName, "tables"));
						}
						else
						{
							col = thisCol;
						}
					}
				}
				if (col != null)
				{
					return addColumn(col);
				}

			}
			throw new SQLException(
					String.format(SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
							columnName, "tables"));
		}
		if (columns.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_, columnName, "table"));
		}
		return addColumn(columns.iterator().next());
	}

	/**
	 * Add a filter to the query.
	 * 
	 * @param filter
	 *            The filter to add.
	 */
	public void addFilter( final Expr filter )
	{
		checkBuilt();
		SparqlQueryBuilder.addFilter(query, filter);
	}

	/**
	 * Add an optional table to the query.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addOptionalTable( final String schemaName,
			final String tableName ) throws SQLException
	{
		checkBuilt();
		return addTable(schemaName, tableName, true);
	}

	/**
	 * Add an order by to the query.
	 * 
	 * @param expr
	 *            The expression to order by.
	 * @param ascending
	 *            true of order should be ascending, false = descending.
	 */
	public void addOrderBy( final Expr expr, final boolean ascending )
	{
		checkBuilt();
		query.addOrderBy(expr, ascending ? Query.ORDER_ASCENDING
				: Query.ORDER_DESCENDING);
	}

	/**
	 * Add a table to the query.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @return The node that represents the table.
	 * @throws SQLException
	 *             if the table is in multiple schemas or not found.
	 */
	public Node addTable( final String schemaName, final String tableName )
			throws SQLException
	{
		checkBuilt();
		return addTable(schemaName, tableName, false);
	}

	public Node addTable( final String schemaName, final String tableName,
			final boolean optional ) throws SQLException
	{
		checkBuilt();
		final Collection<RdfTable> tables = findTables(schemaName, tableName);

		if (tables.size() > 1)
		{
			throw new SQLException(
					String.format(SparqlQueryBuilder.FOUND_IN_MULTIPLE_,
							tableName, "schemas"));
		}
		if (tables.isEmpty())
		{
			throw new SQLException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_ANY_, tableName, "schema"));
		}
		final RdfTable table = tables.iterator().next();
		// make sure the table is in the query.
		RdfTableInfo sti = tablesInQuery.get(table.getSQLName());
		if (sti == null)
		{
			sti = new RdfTableInfo(getElementGroup(), table, optional);
		}
		return sti.getTableVar();
	}

	public void addUsing( final String columnName )
	{
		if (tablesInQuery.size() < 2)
		{
			throw new IllegalArgumentException(
					"There must be at least 2 tables in the query");
		}
		final Iterator<String> iter = tablesInQuery.keySet().iterator();
		final RdfTableInfo rti = tablesInQuery.get(iter.next());
		final RdfColumn baseColumn = rti.getColumn(columnName);
		if (baseColumn == null)
		{
			throw new IllegalArgumentException(String.format(
					"column %s not found in %s", columnName, rti.getSQLName()));
		}
		columnsInUsing.add(columnName);
		try
		{
			final ExprVar left = new ExprVar(addColumn(baseColumn));
			while (iter.hasNext())
			{
				final RdfTableInfo rti2 = tablesInQuery.get(iter.next());
				final RdfColumn col = rti2.getColumn(columnName);
				if (col == null)
				{
					throw new IllegalArgumentException(
							String.format("column %s not found in %s", col,
									rti2.getSQLName()));
				}

				addFilter(new E_Equals(left, new ExprVar(addColumn(col))));
			}
		}
		catch (final SQLException e)
		{
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	/**
	 * Adds the the expression as a variable to the query.
	 * As a variable the result will be returned from the query.
	 * 
	 * @param expr
	 *            The expression that defines the variable
	 * @param name
	 *            the alias for the expression, if null no alias is used.
	 */
	public void addVar( final Expr expr, final String name )
	{
		Column column;
		checkBuilt();
		final NodeValue nv = expr.getConstant();
		if ((name != null) || (nv == null) || !nv.getNode().isVariable())
		{
			final String s = StringUtils.defaultString(expr.getVarName());
			if (s.equals(StringUtils.defaultIfBlank(name, s)))
			{
				column = new ExprColumn(s, expr);
				query.addResultVar(s);
			}
			else
			{
				column = new ExprColumn(name, expr);
				query.addResultVar(name, expr);
			}
		}
		else
		{
			column = new ExprColumn(nv.getVarName(), expr);
			query.addResultVar(nv.getNode());
		}
		query.getResultVars();
		columnsInResult.add(column);
	}

	/**
	 * Adds the the column as a variable to the query.
	 * As a variable the result will be returned from the query.
	 * 
	 * @param col
	 *            Adds a column as a variable.
	 * @param alias
	 *            the alias for the expression, if null no alias is used.
	 * @throws SQLException
	 */
	public void addVar( final RdfColumn col, final String alias )
			throws SQLException
	{
		checkBuilt();

		// figure out what name we are going to use.
		final String sparqlName = StringUtils.defaultString(alias,
				col.getSPARQLName());

		// allocate the var for the real name.
		final Var v = Var.alloc(sparqlName);
		// if we have not seen this var before register all the info.
		if (!query.getResultVars().contains(v.toString()))
		{
			// if an alias is being used then do special registration
			if (sparqlName.equalsIgnoreCase(alias))
			{
				final Node n = addColumn(col);
				final ElementBind bind = new ElementBind(v, new ExprVar(n));
				getElementGroup().addElement(bind);
				// add the alias for the column to the columns list.
				columnsInQuery.put(v.getName(), col);
			}
			query.addResultVar(v);
			query.getResultVars();
			columnsInResult.add(col);
		}
	}

	/**
	 * Adds the the columns as a variables to the query.
	 * As variables the results will be returned from the query.
	 * If there is already a column with the same name in the vars then
	 * the column's full SPARQL DB name will be used as the
	 * alias. If there are multiple columns then no alias is assumed.
	 * 
	 * @param cols
	 *            Columns to add to the query as variables.
	 * @throws SQLException
	 */
	public void addVars( final Iterator<RdfColumn> cols ) throws SQLException
	{
		checkBuilt();
		while (cols.hasNext())
		{
			final RdfColumn col = cols.next();
			addVar(col, getColCount(col) > 1 ? null : col.getName());
		}
	}

	/**
	 * Get the SPARQL query.
	 * 
	 * @return The constructed SPARQL query.
	 */
	public Query build()
	{

		if (!isBuilt)
		{
			if (!catalog.isService())
			{
				// apply the type filters to each subpart.
				for (final RdfTableInfo sti : tablesInQuery.values())
				{
					sti.addTypeFilters();
				}

			}

			// renumber the Bnodes.
			final Element e = new BnodeRenumber().renumber(query
					.getQueryPattern());
			query.setQueryPattern(e);

			if (catalog.isService())
			{
				// create a copy of the query so that we can verify that it is
				// good.
				Query result = query.cloneQuery();

				// create the service call
				// make sure we project all vars for the filters.
				final Set<Var> vars = new HashSet<Var>();
				for (final CheckTypeF f : getTypeFilterList())
				{
					vars.add(f.getArg().asVar());
				}
				result.addProjectVars(vars);
				final ElementService service = new ElementService(
						catalog.getServiceNode(), new ElementSubQuery(result),
						false);
				// create real result
				result = new Query();
				result.setQuerySelectType();
				result.addProjectVars(query.getProjectVars());
				SparqlQueryBuilder.getElementGroup(result).addElement(service);
				for (final CheckTypeF f : getTypeFilterList())
				{
					SparqlQueryBuilder.addFilter(result, f);
				}
				query = result;
			}
			isBuilt = true;
		}
		return query;
	}

	private void checkBuilt()
	{
		if (isBuilt)
		{
			throw new IllegalStateException("Query was already built");
		}
	}

	/**
	 * Find all the columns for the given schema, table, and column patterns
	 * 
	 * @param schemaNamePattern
	 *            The schema name pattern. null = no restriction.
	 * @param tableNamePattern
	 *            The table name pattern. null = no restriction.
	 * @param columnNamePattern
	 *            The column name pattern. null = no restriction.
	 * @return
	 */
	private Collection<RdfColumn> findColumns( final String schemaNamePattern,
			final String tableNamePattern, final String columnNamePattern )
	{
		final List<RdfColumn> columns = new ArrayList<RdfColumn>();
		for (final Schema schema : catalog.findSchemas(schemaNamePattern))
		{
			for (final Table table : schema.findTables(tableNamePattern))
			{
				for (final Column column : table.findColumns(columnNamePattern))
				{
					if (column instanceof RdfColumn)
					{
						columns.add((RdfColumn) column);
					}
				}
			}
		}
		return columns;
	}

	private Collection<RdfTable> findTables( final String schemaName,
			final String tableName )
	{
		final List<RdfTable> tables = new ArrayList<RdfTable>();
		for (final Schema schema : catalog.findSchemas(schemaName))
		{
			for (final Table table : schema.findTables(tableName))
			{
				if (table instanceof RdfTable)
				{
					tables.add((RdfTable) table);
				}
			}
		}
		return tables;
	}

	/**
	 * Get the catalog this builder is working with.
	 * 
	 * @return a Catalog.
	 */
	public RdfCatalog getCatalog()
	{
		return catalog;
	}

	// get the number of columns in the selected tables with the same name
	private int getColCount( final Column col )
	{
		int retval = 0;
		for (final Column c : columnsInQuery.values())
		{
			if (c.getName().equalsIgnoreCase(col.getName()))
			{
				retval++;
			}
		}
		return retval;
	}

	/**
	 * Get a column from the query.
	 * 
	 * None of the parameters may be null.
	 * 
	 * @param schemaName
	 *            The schema name
	 * @param tableName
	 *            The table name
	 * @param columnName
	 *            The column name
	 * @return The RdfColumn or null if no column is defined in the query.
	 */
	public RdfColumn getColumn( final String schemaName,
			final String tableName, final String columnName )
	{
		if (schemaName == null)
		{
			throw new IllegalArgumentException("Schema name may not be null");
		}
		if (tableName == null)
		{
			throw new IllegalArgumentException("Table name may not be null");
		}
		if (columnName == null)
		{
			throw new IllegalArgumentException("Column name may not be null");
		}
		final Iterator<RdfColumn> iter = findColumns(schemaName, tableName,
				columnName).iterator();
		return (iter.hasNext()) ? iter.next() : null;
	}

	private ElementGroup getElementGroup()
	{
		return SparqlQueryBuilder.getElementGroup(query);
	}

	/*
	 * private String getTableVar( Table table )
	 * {
	 * return String.format( "TABLE_%s_%s_%s",
	 * table.getCatalog().getLocalName(),
	 * table.getSchema().getLocalName(), table.getLocalName() );
	 * }
	 */
	private Node getNodeBySQLName( final String dbName )
	{
		final Node n = nodesInQuery.get(dbName);
		if (n == null)
		{
			throw new IllegalStateException(String.format(
					SparqlQueryBuilder.NOT_FOUND_IN_QUERY, dbName));
		}
		return n;
	}

	public RdfColumn getNodeColumn( final Node n )
	{
		checkBuilt();
		for (final Map.Entry<String, Node> entry : nodesInQuery.entrySet())
		{
			if (entry.getValue().equals(n))
			{
				final String objectSQLName = entry.getKey();
				return columnsInQuery.get(objectSQLName);
			}
		}
		return null;
	}

	public RdfTableInfo getNodeTable( final Node n )
	{
		checkBuilt();
		for (final Map.Entry<String, Node> entry : nodesInQuery.entrySet())
		{
			if (entry.getValue().equals(n))
			{
				final String objectSQLName = entry.getKey();
				RdfTableInfo sti = tablesInQuery.get(objectSQLName);
				if (sti == null)
				{
					final RdfColumn col = columnsInQuery.get(objectSQLName);
					if (col != null)
					{
						sti = tablesInQuery.get(col.getTable().getSQLName());
					}
				}
				return sti;
			}
		}
		return null;
	}

	public List<Column> getResultColumns()
	{
		return columnsInResult;
	}

	/**
	 * Get the table definition for the specified namespace and name.
	 * The Tabledef will not have a query segment.
	 * 
	 * @param namespace
	 *            The namespace for the table definition.
	 * @param localName
	 *            The name for the table definition.
	 * @return The table definition.
	 */
	public RdfTableDef getTableDef( final String namespace,
			final String localName )
	{
		final RdfTableDef.Builder builder = new RdfTableDef.Builder();
		final Model model = ModelFactory.createDefaultModel();

		// final RdfTableDef tableDef = new RdfTableDef(namespace,
		// localName, "", null);
		final VarExprList expLst = query.getProject();
		new ArrayList<Column>();
		for (final Var var : expLst.getVars())
		{
			String varColName = null;
			final Expr expr = expLst.getExpr(var);
			if (expr != null)
			{
				varColName = expr.getExprVar().getVarName()
						.replace(NameUtils.SPARQL_DOT, ".");
			}
			else
			{
				varColName = var.getName().replace(NameUtils.SPARQL_DOT, ".");
			}
			final Column c = columnsInQuery.get(varColName);
			if (c == null)
			{
				throw new IllegalStateException(String.format(
						SparqlQueryBuilder.NOT_FOUND_IN_QUERY, var));
			}
			builder.addColumnDef(c.getColumnDef());

		}
		return builder.build(model);
	}

	private Set<CheckTypeF> getTypeFilterList()
	{
		final Set<CheckTypeF> retval = new HashSet<CheckTypeF>();
		for (final RdfTableInfo sti : tablesInQuery.values())
		{
			retval.addAll(sti.getTypeFilterList());
		}
		return retval;
	}

	/**
	 * @return returns true if the query is going to return all the columns from
	 *         all the tables.
	 */
	public boolean isAllColumns()
	{
		checkBuilt();
		return query.isQueryResultStar();
	}

	/**
	 * Sets all the columns for all the tables currently defined.
	 * This method should be called after all tables have been added to the
	 * querybuilder.
	 * 
	 * @throws SQLException
	 */
	public void setAllColumns() throws SQLException
	{
		checkBuilt();
		int i = 0;
		for (final RdfTableInfo t : tablesInQuery.values())
		{

			final Iterator<RdfColumn> iter = t.getColumns();
			while (iter.hasNext())
			{
				final RdfColumn col = iter.next();
				col.getName();
				if ((i == 0) || !columnsInUsing.contains(col.getName()))
				{
					addColumn(col);
					addVar(col, i == 0 ? col.getName()
							: getColCount(col) > 1 ? null : col.getName());
				}
			}
			i++;
		}
	}

	/**
	 * Sets the distinct flag for the SPARQL query.
	 */
	public void setDistinct()
	{
		checkBuilt();
		query.setDistinct(true);
	}

	/**
	 * Sets the limit for the SPARQL query.
	 * 
	 * @param limit
	 *            The number of records to return
	 */
	public void setLimit( final Long limit )
	{
		checkBuilt();
		query.setLimit(limit);
	}

	/**
	 * Set the offset for the SPARQL query.
	 * 
	 * @param offset
	 *            The number of rows to skip over.
	 */
	public void setOffset( final Long offset )
	{
		checkBuilt();
		query.setOffset(offset);
	}

	public void setOrderBy( final RdfKey key )
	{
		final List<Var> vars = query.getProjectVars();
		for (final RdfKeySegment seg : key.getSegments())
		{
			query.addOrderBy(vars.get(seg.getIdx()),
					seg.isAscending() ? Query.ORDER_ASCENDING
							: Query.ORDER_DESCENDING);
		}
	}

	/**
	 * Return the SPARQL query as the string value for the builder.
	 */
	@Override
	public String toString()
	{
		return String.format("QueryBuilder%s[%s]", (isBuilt ? "(builtargs)"
				: ""), query.toString());
	}

}
