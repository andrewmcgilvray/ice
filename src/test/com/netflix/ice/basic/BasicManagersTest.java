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
package com.netflix.ice.basic;

import com.google.common.collect.Maps;
import com.netflix.ice.common.ConsolidateType;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.tag.Product;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class BasicManagersTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void keysForProductsWithDuplicateNames() {
        ProductService ps = new BasicProductService();

        Product a = ps.getProduct("CommonName", "ServiceCodeA");
        Product b = ps.getProduct("CommonName", "ServiceCodeB");

        assertNotEquals("Products should not be equal", a, b);
        assertNotEquals("Products should not be equal", a.hashCode(), b.hashCode());
        assertFalse("Product.equals failed", a.equals(b));
        assertTrue("Product.compareTo failed", a.compareTo(b) < 0);

        BasicManagers.Key keyA = new BasicManagers.Key(a, ConsolidateType.daily);
        BasicManagers.Key keyB = new BasicManagers.Key(b, ConsolidateType.daily);

        assertNotEquals("Keys should not be equal", keyA, keyB);
        assertNotEquals("Keys should not be equal", keyA.hashCode(), keyB.hashCode());
        assertFalse("Key.equals failed", keyA.equals(keyB));
        assertTrue("Key.compareTo failed", keyA.compareTo(keyB) < 0);

        Map<BasicManagers.Key, Boolean> m = Maps.newTreeMap();
        m.put(keyA, true);
        assertFalse("Key collision in map", m.containsKey(keyB));
        m.put(keyB, true);
        assertTrue("Wrong number of entries in map", m.size() == 2);
    }
}
