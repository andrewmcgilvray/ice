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

import java.io.File;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;

public class KubernetesReportTest {
	private static final String resourceDir = "src/test/resources/";

	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(DateTime month, KubernetesConfig config, ResourceService rs) {
			super(null, null, month, config, rs);
		}
	}

	@Test
	public void testReadFile() {
        String[] customTags = new String[]{"Tag1", "Tag2", "Tag3"};
        KubernetesConfig kc = new KubernetesConfig();
        kc.setTags(Lists.newArrayList(customTags));
        ResourceService rs = new BasicResourceService(new BasicProductService(), customTags, false);
		TestKubernetesReport tkr = new TestKubernetesReport(new DateTime("2019-01", DateTimeZone.UTC), kc, rs);
		
		File file = new File(resourceDir, "kubernetes-2019-01.csv");
		tkr.readFile(file);
		
		assertEquals("Wrong number of clusters", 4, tkr.getClusters().size());
		assertEquals("Should not have data at hour 0", 0, tkr.getData("dev-usw2a", 0).size());
		assertEquals("Should have data at hour 395", 10, tkr.getData("dev-usw2a", 395).size());
		
		List<String[]> data = tkr.getData("dev-usw2a", 395);
		
		// find the kube-system namespace
		String[] kubeSystem = null;
		for (String[] item: data) {
			if (tkr.getString(item, KubernetesColumn.Namespace).equals("kube-system")) {
				kubeSystem = item;
				break;
			}
		}
		
		assertNotNull("Missing item in report", kubeSystem);
		class ItemValue {
			KubernetesColumn col;
			String value;
			
			ItemValue(KubernetesColumn c, String v) {
				col = c;
				value = v;
			}
		}
		ItemValue[] itemValues = new ItemValue[]{
				new ItemValue(KubernetesColumn.StartDate, "2019-01-17T11:00:00Z"),
				new ItemValue(KubernetesColumn.EndDate, "2019-01-17T12:00:00Z"),
				new ItemValue(KubernetesColumn.RequestsCPUCores, "1.960000000000001"),
				new ItemValue(KubernetesColumn.UsedCPUCores, "0.09185591484457487"),
				new ItemValue(KubernetesColumn.LimitsCPUCores, "1.7000000000000008"),
				new ItemValue(KubernetesColumn.ClusterCPUCores, "156"),
				new ItemValue(KubernetesColumn.RequestsMemoryGiB, "2.158203125"),
				new ItemValue(KubernetesColumn.UsedMemoryGiB, "1.7474441528320312"),
				new ItemValue(KubernetesColumn.LimitsMemoryGiB, "5.87890625"),
				new ItemValue(KubernetesColumn.ClusterMemoryGiB, "576.1466674804688"),
				new ItemValue(KubernetesColumn.NetworkInGiB, "0.0016675007839997604"),
				new ItemValue(KubernetesColumn.ClusterNetworkInGiB, "0.004905043024983669"),
				new ItemValue(KubernetesColumn.NetworkOutGiB, "0.00102091437826554"),
				new ItemValue(KubernetesColumn.ClusterNetworkOutGiB, "0.003215055426130298"),
				new ItemValue(KubernetesColumn.PersistentVolumeClaimGiB, "0"),
				new ItemValue(KubernetesColumn.ClusterPersistentVolumeClaimGiB, "308"),
		};
		for (ItemValue iv: itemValues) {
			assertEquals("Wrong value for " + iv.col, iv.value, tkr.getString(kubeSystem, iv.col));	
		}
	}

}
