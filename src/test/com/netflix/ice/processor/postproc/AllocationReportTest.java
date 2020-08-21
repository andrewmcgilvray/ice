package com.netflix.ice.processor.postproc;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.ice.processor.postproc.AllocationReport.Key;

public class AllocationReportTest {

	@Test
	public void testKey() {
		// Test that we get the same hashcode for different objects with the same strings
		List<String> tags1 = Lists.newArrayList(new String[]{"foo", "bar"});
		List<String> tags2 = Lists.newArrayList(new String[]{"foo", "bar"});
		
		Key key1 = new Key(tags1);
		Key key2 = new Key(tags2);
		assertEquals("hashcodes don't match", key1.hashCode(), key2.hashCode());
		
		Map<Key, String> m = Maps.newHashMap();
		m.put(key1, "foo");
		m.put(key2, "bar");
		assertEquals("should only have one entry in map", 1, m.size());
	}

}
