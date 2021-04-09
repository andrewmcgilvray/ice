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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class UsageType extends Tag {
	private static final long serialVersionUID = 1L;
	//private static final Logger logger = LoggerFactory.getLogger(UsageType.class);
	public static final String multiAZ = ".multiaz";
    public final String unit;

    private UsageType (String name, String unit) {
        super(name);
        this.unit = unit;
    }
    private static ConcurrentMap<String, UsageType> usageTypes = Maps.newConcurrentMap();

    public static void serialize(DataOutput out, UsageType usageType) throws IOException {
        out.writeUTF(usageType == null ? "" : usageType.name);
        out.writeUTF(usageType == null ? "" : usageType.unit);
    }

    public static void serializeCsvHeader(Writer out) throws IOException {
    	out.write("usageType,usageUnits");
    }
    
    public static void serializeCsv(Writer out, UsageType usageType) throws IOException {
        out.write((usageType == null ? "" : usageType.name) + ",");
        out.write(usageType == null ? "" : usageType.unit);
    }

    public static UsageType deserialize(DataInput in) throws IOException {
        String name = in.readUTF();
        String unit = in.readUTF();
        if (name.isEmpty() && unit.isEmpty())
        	return null;
        
        return getUsageType(name, unit);
    }

    public static UsageType getUsageType(String name, String unit) {
        if (name.isEmpty()) {
            // Some line items don't have a value for usageType
            name = "None";
            unit = "";
        }
    	else if (unit == null) {
    		logger.error("getUsageType called with null unit");
    		new Exception().printStackTrace();
    		unit = "";
    	}
        UsageType usageType = usageTypes.get(name);
        if (usageType == null) {
            usageTypes.putIfAbsent(name, new UsageType(name, unit));
            usageType = usageTypes.get(name);
        }
// We don't care about units at this time.
//        else if (!usageType.unit.equals(unit)) {
//            logger.error("found different units for " + usageType + ", " + usageType.unit + ", " + unit);
//        }
        return usageType;
    }

    public static List<UsageType> getUsageTypes(List<String> names) {
        List<UsageType> result = Lists.newArrayList();
        for (String name: names)
            result.add(usageTypes.get(name));
        return result;
    }
    
    public boolean isMultiAZ() {
    	return name.contains(multiAZ);
    }
}
