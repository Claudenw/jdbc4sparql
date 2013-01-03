package org.xenei.jdbc4sparql.iface;

public interface ColumnDef
{
	String getColumnClassName();

	int getDisplaySize();

	String getLabel();

	int getNullable();

	int getPrecision();

	int getScale();

	int getType();

	String getTypeName();

	boolean isAutoIncrement();

	boolean isCaseSensitive();

	boolean isCurrency();

	boolean isDefinitelyWritable();

	boolean isReadOnly();

	boolean isSearchable();

	boolean isSigned();

	boolean isWritable();
}
