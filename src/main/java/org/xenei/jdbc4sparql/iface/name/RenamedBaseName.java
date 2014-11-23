package org.xenei.jdbc4sparql.iface.name;

import org.apache.commons.lang.StringUtils;

public class RenamedBaseName implements BaseName {
	private BaseName baseName;
	private BaseName newName;
	private Integer _hashCode;

	public RenamedBaseName(BaseName baseName, String catalog, String schema,
			String table, String column) {
		this.baseName = baseName;
		this.newName = new BaseNameImpl(catalog, schema, table, column);
		this._hashCode = null;
	}

	/**
	 * Get the column name string
	 *
	 * @return
	 */
	@Override
	public String getCatalog() {
		return StringUtils.defaultString(newName.getCatalog(),
				baseName.getCatalog());
	}

	@Override
	public String getSchema() {
		return StringUtils.defaultString(newName.getSchema(),
				baseName.getSchema());
	}

	@Override
	public String getTable() {
		return StringUtils.defaultString(newName.getTable(),
				baseName.getTable());
	}

	@Override
	public String getCol() {
		return StringUtils.defaultString(newName.getCol(), baseName.getCol());
	}

	@Override
	public int hashCode() {
		if (_hashCode == null) {
			_hashCode = BaseName.Comparator.hashCode(this);
		}
		return _hashCode;
	}

	@Override
	public boolean equals(Object o) {
		return BaseName.Comparator.equals(this, o);
	}

	@Override
	public String getGUID() {
		return baseName.getGUID();
	}

}
