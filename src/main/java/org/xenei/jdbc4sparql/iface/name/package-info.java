package org.xenei.jdbc4sparql.iface.name;

/*
 * the name package contains Names for items in the system.  All names are fully qualified 
 * as far as possible by catalog, schema, table and column.  In addition all items have a GUID that is based
 * on the name segments.
 * 
 * When the name is displayed it may be either the GUID or the fully qualified name with a specified character
 * for seperation.  The character specification is needed as SPARQL and SQL have different requirements for the
 * character to make a valid name.
 */
