package org.xenei.jdbc4sparql.impl.virtual;

/*
 * the virtual package contains catalog, schema and table definitions for virtual entities.
 * 
 * Virtual entities are entities that do not exist in any catalog but are created during execution.  For example 
 * <code>select SUM( col1 ) </code>
 * 
 * creates a column to represent the <code>SUM( col1 )</code>, that column is in the virtual table.
 */
