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

public enum InstanceOs {
    linux("", "", "Linux/UNIX", false),
    linsqlweb(".linsqlweb", ":0200", "Linux with SQL Server Web", false),
    linsqlstd(".linsqlstd", ":0004", "Linux with SQL Server Standard", false),
    linsqlent(".linsqlent", ":0100", "Linux with SQL Server Enterprise", false),
    winsqlweb(".winsqlweb", ":0202", "Windows with SQL Server Web", false),
    winsqlstd(".winsqlstd", ":0006", "Windows with SQL Server Standard", false),
    winsqlent(".winsqlent", ":0102", "Windows with SQL Server Enterprise", false),
    sles(".sles", ":000g", "SUSE Linux", false),
    rhel(".rhel", ":0010", "Red Hat Enterprise Linux", false),
    rhelsqlweb(".rhelsqlweb", ":0210", "Red Hat Enterprise Linux with SQL Server Web", false),
    rhelsqlstd(".rhelsqlstd", ":0014", "Red Hat Enterprise Linux with SQL Server Standard", false),
    rhelsqlent(".rhelsqlent", ":0110", "Red Hat Enterprise Linux with SQL Server Enterprise", false),
    rhelha(".rhelha", ":1010", "Red Hat Enterprise Linux with HA", false),
    rhelhasqlstd(".rhelhasqlstd", ":1014", "Red Hat Enterprise Linux with HA with SQL Server Standard", false),
    rhelhasqlent(".rhelhasqlent", ":1110", "Red Hat Enterprise Linux with HA with SQL Server Enterprise", false),
    rhbl(".rhbl", ":00g0", "Red Hat BYOL Linux", false),
    windows(".windows", ":0002", "Windows", false),
    winbyol(".winbyol", ":0800", "Windows BYOL", false),
    dw(".dw", ":0001", "redshift", false),
    
    // Spot Instance - Number is the Zone #
    // VPC Spot instance starts with SV followed by three digit zone number - ":SV000"
    spot("", ":S0000", "Linux Spot Instance", true),
    spotwin(".windows", ":0002:S0000", "Windows Spot Instance", true),
    
    others(".others", ":others", "others", false);

    public final String usageType;
    public final String code;
    public final String description;
    public final boolean isSpot;

    InstanceOs(String usageType, String code, String description, boolean isSpot) {
        this.usageType = usageType;
        this.code = code.toLowerCase();
        this.description = description.toLowerCase();
        this.isSpot = isSpot;
    }
    
    public static InstanceOs withCode(String code) {
        for (InstanceOs os: InstanceOs.values()) {
            if (code.toLowerCase().equals(os.code))
                return os;
        }
        if (code.startsWith(":S"))
        	return spot;
        if (code.startsWith(":0002:S"))
        	return spotwin;
        
        return others;
    }

    /**
     * withDescription() returns the InstanceOs value based on the product description in a
     * EC2 ReservedInstance object.
     */
    public static InstanceOs withDescription(String description) {
    	String descLC = description.toLowerCase();
        for (InstanceOs os: InstanceOs.values()) {
            if (descLC.startsWith(os.description.toLowerCase()))
                return os;
        }
        return others;
    }
}
