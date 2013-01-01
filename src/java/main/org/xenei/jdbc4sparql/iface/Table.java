package org.xenei.jdbc4sparql.iface;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.xenei.jdbc4sparql.impl.ColumnImpl;

public interface Table extends NamespacedObject, TableDef
{
	/**
	 * The schema the table belongs in.
	 * @return
	 */
	Schema getSchema();
	
	Catalog getCatalog();
	
	/**
	 * Get the type of table.
	 * Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", 
	 * "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	 */
	String getType();  
	
	Iterator<Column> getColumns();
	
	Column getColumn(int idx);
	
	Column getColumn(String name);
	
	NameFilter<Column> findColumns(String columnNamePattern);
	
	String getDBName();
	
	public static class ColumnIterator implements Iterator<Column> {
		private Table table;
		private String namespace;
		private Iterator<? extends ColumnDef> iter;
		
		public ColumnIterator( Table table, Collection<? extends ColumnDef> def)
		{
			this( table.getNamespace(), table, def );
		}
		
		public ColumnIterator(String namespace, Table table, Collection<? extends ColumnDef> def)
		{
			this.table = table;
			this.namespace = namespace;
			iter = def.iterator();
		}

		@Override
		public boolean hasNext()
		{
			return iter.hasNext();
		}

		@Override
		public Column next()
		{
			return new ColumnImpl( namespace, table, iter.next());
		}

		@Override
		public void remove()
		{
			// TODO Auto-generated method stub
			
		}
		
	}
}
