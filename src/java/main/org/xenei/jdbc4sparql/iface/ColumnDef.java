package org.xenei.jdbc4sparql.iface;

public interface ColumnDef 
{
	String getColumnClassName();
	int getDisplaySize();
	String getLabel();
	int getType();
	String getTypeName();
	int getPrecision();
	int getScale();
	boolean isAutoIncrement();
	boolean isCaseSensitive();
	boolean isCurrency();
	boolean isDefinitelyWritable();
	int getNullable();
	boolean isReadOnly();
	boolean isSearchable();
	boolean isSigned();
	boolean isWritable();
}
