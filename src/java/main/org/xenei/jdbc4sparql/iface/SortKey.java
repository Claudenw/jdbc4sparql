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
package org.xenei.jdbc4sparql.iface;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortKey implements Comparator<Object[]>
{
	private boolean unique;
	private final List<KeySegment> segments;

	public SortKey()
	{
		segments = new ArrayList<KeySegment>();
	}

	public SortKey addSegment( final KeySegment segment )
	{
		segments.add(segment);
		return this;
	}

	@Override
	public int compare( final Object[] data1, final Object[] data2 )
	{
		for (final KeySegment segment : segments)
		{
			final int retval = segment.compare(data1, data2);
			if (retval != 0)
			{
				return retval;
			}
		}
		return 0;
	}

	public boolean isUnique()
	{
		return unique;
	}

	public void setUnique()
	{
		unique = true;
	}
}
