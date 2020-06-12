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
package com.netflix.ice.tag;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import com.netflix.ice.tag.ResourceGroup.ResourceException;

public class ResourceGroupTest {

	@Test
	public void testGetResourceGroup() throws ResourceException {
		ResourceGroup rg = ResourceGroup.getResourceGroup(new String[]{"foo"});
		assertEquals("single name incorrect", "foo", rg.getUserTags()[0].name);		
	
		rg = ResourceGroup.getResourceGroup(new String[]{"", "foo"});
		UserTag[] userTags = rg.getUserTags();
		assertEquals("group with only second tag name incorrect(0)", "", userTags[0].name);		
		assertEquals("group with only second tag name incorrect(1)", "foo", userTags[1].name);

		rg = ResourceGroup.getResourceGroup(new String[]{null, "foo"});
		userTags = rg.getUserTags();
		assertEquals("group with only second tag name incorrect(0)", "", userTags[0].name);		
		assertEquals("group with only second tag name incorrect(1)", "foo", userTags[1].name);

		rg = ResourceGroup.getResourceGroup((String[]) null);
		assertNull("should be null", rg);
	}
	
	@Test
	public void testSerialization() throws IOException, ResourceException {
		ResourceGroup rg = ResourceGroup.getResourceGroup(new String[]{"foo", null, "bar", null, null});
		
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(output);
		
		ResourceGroup.serialize(out, rg);
		
		ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
		DataInput in = new DataInputStream(input);
		ResourceGroup rg1 = ResourceGroup.deserialize(in, rg.getUserTags().length);

		assertEquals("wrong number of tags", rg.getUserTags().length, rg1.getUserTags().length);
		assertEquals("resource group doesn't match after serialize/deserialize", rg, rg1);
	}
}
