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
package org.xenei.jdbc4sparql.meta;

import java.sql.DatabaseMetaData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xenei.jdbc4sparql.iface.ColumnDef;

public class MetaTableDefTest
{
	private TableDefImpl def;

	@Before
	public void setUp()
	{
		def = new TableDefImpl(MetaNamespace.NS, "TestDef", null);
		def.add(ColumnDefImpl.Builder
				.getStringBuilder(MetaNamespace.NS, "NULLABLE_STRING")
				.setNullable(DatabaseMetaData.columnNullable).build());
		def.add(ColumnDefImpl.Builder.getStringBuilder(MetaNamespace.NS,
				"STRING").build());
		def.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "INT").build());
		def.add(ColumnDefImpl.Builder
				.getIntegerBuilder(MetaNamespace.NS, "NULLABLE_INT")
				.setNullable(DatabaseMetaData.columnNullable).build());
	}

	@Test
	public void testAddColumns()
	{
		Assert.assertEquals(4, def.getColumnCount());
		Assert.assertEquals("NULLABLE_STRING", def.getColumnDef(0).getLabel());
		Assert.assertEquals("STRING", def.getColumnDef(1).getLabel());
		Assert.assertEquals("INT", def.getColumnDef(2).getLabel());
		Assert.assertEquals("NULLABLE_INT", def.getColumnDef(3).getLabel());
		Assert.assertNull(def.getSortKey());
	}

	@Test
	public void testAddSortKey()
	{
		def.addKey("STRING");
		Assert.assertNotNull(def.getSortKey());
		Assert.assertFalse(def.getSortKey().isUnique());
		def.setUnique();
		Assert.assertTrue(def.getSortKey().isUnique());
	}

	@Test
	public void testGetColumn()
	{
		final ColumnDef col = def.getColumnDef("STRING");
		Assert.assertEquals(def.getColumnDef(1), col);
	}

	@Test
	public void testVerify()
	{
		Object[] row = new Object[] { "string1", "string", 5, 10 };
		def.verify(row);
		row = new Object[] { null, "string", 5, null };
		def.verify(row);
		row = new Object[] { null, "string", 5L, null };
		try
		{
			def.verify(row);
		}
		catch (final IllegalArgumentException expected)
		{ // do nothing
		}
		row = new Object[] { "string", null, 5, null };
		try
		{
			def.verify(row);
		}
		catch (final IllegalArgumentException expected)
		{ // do nothing
		}
	}

}
