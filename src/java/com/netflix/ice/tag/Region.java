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
import com.netflix.ice.tag.Zone.BadZone;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class Region extends Tag {
	private static final long serialVersionUID = 1L;
	
    private static ConcurrentMap<String, Region> regionsByName = Maps.newConcurrentMap();
    private static ConcurrentMap<String, Region> regionsByShortName = Maps.newConcurrentMap();

    public static List<String> cloudFrontRegions = Lists.newArrayList(new String[]{"AP","AU","CA","EU","IN","JP","ME","SA","US","ZA"});
	
	public static final Region GLOBAL = new Region("global", "", "Global") {
		private static final long serialVersionUID = 1L;
		// Always put global first unless comparing to aggregated tag.
		@Override
        public int compareTo(Tag t) {
	        if (t == aggregated)
	            return -t.compareTo(this);
            return this == t ? 0 : -1;
        }		
	};	
	
	public static final Region US_EAST_1 = new Region("us-east-1", "USE1", "US East (N. Virginia)");
	public static final Region US_EAST_1_WL1_BOS = new Region("us-east-1-wl1-bos-wlz-1", "USE1WL1", "US East (Verizon) - Boston");
    public static final Region US_EAST_2 = new Region("us-east-2", "USE2", "US East (Ohio)");
    public static final Region US_WEST_1 = new Region("us-west-1", "USW1", "US West (N. California)");
    public static final Region US_WEST_2 = new Region("us-west-2", "USW2", "US West (Oregon)");
    public static final Region US_WEST_2_LAX_1 = new Region("us-west-2-lax-1", "LAX1", "US West (Los Angeles)");
    public static final Region US_WEST_2_WL1_SFO = new Region("us-west-2-wl1-sfo-wlz-1", "USW2WL1", "US West (Verizon) - San Francisco Bay Area");
    public static final Region CA_CENTRAL_1 = new Region("ca-central-1", "CAN1", "Canada (Central)");
    public static final Region EU_WEST_1 = new Region("eu-west-1", "EU", "EU (Ireland)");
    public static final Region EU_CENTRAL_1 = new Region("eu-central-1", "EUC1", "EU (Frankfurt)");
    public static final Region EU_WEST_2 = new Region("eu-west-2", "EUW2", "EU (London)");
    public static final Region EU_WEST_3 = new Region("eu-west-3", "EUW3", "EU (Paris)");
    public static final Region EU_NORTH_1 = new Region("eu-north-1", "EUN1", "EU (Stockholm)");
    public static final Region EU_SOUTH_1 = new Region("eu-south-1", "EUS1", "EU (Milan)");
    public static final Region AP_EAST_1 = new Region("ap-east-1", "APE1", "Asia Pacific (Hong Kong)");
    public static final Region AP_NORTHEAST_1 = new Region("ap-northeast-1","APN1", "Asia Pacific (Tokyo)");
    public static final Region AP_NORTHEAST_2 = new Region("ap-northeast-2","APN2", "Asia Pacific (Seoul)");
    public static final Region AP_NORTHEAST_3 = new Region("ap-northeast-3","APN3", "Asia Pacific (Osaka-Local)");
    public static final Region AP_SOUTHEAST_1 = new Region("ap-southeast-1", "APS1", "Asia Pacific (Singapore)");
    public static final Region AP_SOUTHEAST_2 = new Region("ap-southeast-2", "APS2", "Asia Pacific (Sydney)");
    public static final Region AP_SOUTH_1 = new Region("ap-south-1", "APS3", "Asia Pacific (Mumbai)");
    public static final Region SA_EAST_1 = new Region("sa-east-1", "SAE1", "South America (Sao Paulo)");
    public static final Region ME_SOUTH_1 = new Region("me-south-1", "MES1", "Middle East (Bahrain)");
    public static final Region AF_SOUTH_1 = new Region("af-south-1", "AFS1", "Africa (Cape Town)");

    static {
        // Populate regions used to serve edge locations for CloudFront
        regionsByShortName.put("US", US_EAST_1);		/* US United States*/
        regionsByShortName.put("CA", CA_CENTRAL_1);		/* CA Canada */
        regionsByShortName.put("EU", EU_WEST_1);		/* EU Europe */
        regionsByShortName.put("JP", AP_NORTHEAST_1);	/* JP Japan */
        regionsByShortName.put("AP", AP_SOUTHEAST_1);	/* AP Asia Pacific */
        regionsByShortName.put("AU", AP_SOUTHEAST_2);	/* AU Australia */
        regionsByShortName.put("IN", AP_SOUTH_1);		/* IN India */
        regionsByShortName.put("SA", SA_EAST_1);		/* SA South America */
        regionsByShortName.put("ME", EU_CENTRAL_1);		/* ME Middle East */
        regionsByShortName.put("ZA", EU_WEST_2);		/* ZA South Africa */
    }

    public final String shortName;
    public final String priceListName;
    Map<String, Zone> zones = Maps.newConcurrentMap();

    private Region(String name, String shortName, String priceListName) {
        super(name);
        this.shortName = shortName;
        this.priceListName = priceListName;
        
        if (!this.shortName.isEmpty())
        	regionsByShortName.put(this.shortName, this);
        regionsByName.put(this.name, this);
    }

    @Override
    public int compareTo(Tag t) {
        if (t instanceof Region) {
        	Region r = (Region) t;
        	if (r == GLOBAL)
        		return -r.compareTo(this);
            int result = this.getName().compareTo(t.getName());
            return result;
        }
        else
            return super.compareTo(t);
    }

    public Collection<Zone> getZones() {
        return zones.values();
    }
    
    public Zone getZone(String name) throws BadZone {
    	Zone zone = zones.get(name);
    	if (zone == null) {
        	zone = Zone.addZone(this, name);
        	if (zone != null)
        		zones.put(name, zone);
    	}
    	
    	return zone;
    }

    public static Region getRegionByShortName(String shortName) {
        return regionsByShortName.get(shortName);
    }

    public static Region getRegionByName(String name) {
        Region region = regionsByName.get(name);
        if (region == null)
        	logger.error("Unknown region name: " + name);
        return region;
    }

    public static List<Region> getRegions(List<String> names) {
        List<Region> result = Lists.newArrayList();
        for (String name: names)
            result.add(regionsByName.get(name));
        return result;
    }

    public static List<Region> getAllRegions() {
        return Lists.newArrayList(regionsByName.values());
    }
}
