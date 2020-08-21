/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.processor.kubernetes;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.tag.UserTag;

public class ClusterNameBuilderTest {
	private static final List<String> tags = Lists.newArrayList(new String[]{ "Tag1", "Tag2", "Tag3" });


	@Test
	public void testNoFuncs() {
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag3"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("Rule with no functions failed", "Three", name);
	}
	
	@Test
	public void testLiteral() {
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("\"foobar\""), null);
		String name = cnb.getClusterNames(null).iterator().next();
		assertEquals("ToUpper failed", "foobar", name);
	}
	
	@Test
	public void testToUpper() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toUpper()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("ToUpper failed", "TWO", name);
	}

	@Test
	public void testToLower() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toLower()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("ToLower failed", "two", name);
	}

	@Test
	public void testRegex() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.regex(\"Stripme-(.*)\")"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("Regex failed", "Two", name);
	}

	@Test
	public void testRegexWithToLower() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.regex(\"Stripme-(.*)\").toLower()"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("Regex failed", "two", name);
	}
	
	@Test
	public void testMultipleTagRules() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")"), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("Three")};
		String name = cnb.getClusterNames(userTags).iterator().next();
		assertEquals("Regex failed", "one-Two", name);
	}
	
	@Test
	public void testEmptyTags() {		
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList("Tag2.toUpper()"), tags);
		UserTag[] userTags = new UserTag[]{ null, null, null};
		assertEquals("Should not return any cluster names", 0, cnb.getClusterNames(userTags).size());
	}

	@Test
	public void testMultipleFormulae() {
		String[] formulae = new String[]{ "Tag1.toLower()+Tag2.regex(\"Stripme(-.*)\")", "Tag3.regex(\"k8s-(.*)\")" };
		ClusterNameBuilder cnb = new ClusterNameBuilder(Lists.newArrayList(formulae), tags);
		UserTag[] userTags = new UserTag[]{ UserTag.get("One"), UserTag.get("Stripme-Two"), UserTag.get("k8s-Three")};
		Set<String> names = cnb.getClusterNames(userTags);
		assertEquals("Wrong number of cluster names", 2, names.size());
		assertTrue("Regex failed for one-Two", names.contains("one-Two"));
		assertTrue("Regex failed for Three", names.contains("Three"));
	}
}
