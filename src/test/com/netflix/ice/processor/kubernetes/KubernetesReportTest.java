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
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.basic.BasicResourceService;
import com.netflix.ice.common.ResourceService;
import com.netflix.ice.processor.config.KubernetesConfig;
import com.netflix.ice.processor.config.S3BucketConfig;
import com.netflix.ice.processor.kubernetes.KubernetesReport.KubernetesColumn;
import com.netflix.ice.processor.postproc.AllocationConfig;
import com.netflix.ice.processor.postproc.RuleConfig;
import com.netflix.ice.tag.UserTag;

public class KubernetesReportTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String resourceDir = "src/test/resources/";

	class TestKubernetesReport extends KubernetesReport {

		public TestKubernetesReport(AllocationConfig config, DateTime month, ResourceService rs) throws Exception {
			super(config, month, rs);
		}
	}
	
	@Test
	public void testProductServiceCodes() {
		String[] codes = new String[]{"EC2Instance", "AmazonCloudWatch", "EBS", "AWSDataTransfer"};
		assertArrayEquals("bad product list", codes, KubernetesReport.productServiceCodes.toArray());
	}
	
	@Test(expected = Exception.class)
	public void testBadBucketConfig() throws Exception {
        String[] customTags = new String[]{"Tag1", "Tag2", "Tag3"};
        AllocationConfig ac = new AllocationConfig();
        ac.setS3Bucket(new S3BucketConfig());
        ac.setKubernetes(new KubernetesConfig());
        ResourceService rs = new BasicResourceService(new BasicProductService(), customTags, false);
  		new TestKubernetesReport(ac, new DateTime("2019-01", DateTimeZone.UTC), rs);		
	}

	@Test
	public void testReadFile() throws Exception {
        String[] customTags = new String[]{"Tag1", "Tag2", "Tag3"};
        AllocationConfig ac = new AllocationConfig();
        S3BucketConfig bc = new S3BucketConfig().withName("test-bucket").withRegion("us-east-1").withAccountId("123456789012");
        ac.setS3Bucket(bc);
        ac.setKubernetes(new KubernetesConfig());
        ResourceService rs = new BasicResourceService(new BasicProductService(), customTags, false);
        
		TestKubernetesReport tkr = new TestKubernetesReport(ac, new DateTime("2019-01", DateTimeZone.UTC), rs);
		
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
	
	@Test
	public void testClusterNameLiteral() throws Exception {
		String yaml = "" +
		"name: kubernetes-breakout\n" + 
		"start: 2019-11\n" + 
		"end: 2022-11\n" + 
		"in:\n" + 
		"  type: cost\n" + 
		"  product: Product\n" + 
		"  userTags:\n" + 
		"    Role: compute\n" + 
	    "allocation: # Perform allocations provided through an allocation report (only one of allocation or results may be present)\n" +
	    "  s3Bucket:\n" +
	    "    name: k8s-report-bucket\n" +
	    "    region: us-east-1\n" +
	    "    prefix: hourly/kubernetes\n" +
	    "    accountId: 123456789012\n" +
	    "    accessRole: ice-role\n" +
	    "    externalId: 234567890123\n" +
	    "  kubernetes: # use the kubernetes precprocessor i.e. preprocess a Kubernetes report into an Allocation report.\n" +
	    "    clusterNameFormulae: [ '\"literal-cluster\"' ]\n" +
	    "  type: cost\n" +
	    "  in:\n" +
		"    Cluster: Cluster\n" +
		"    _Product: _Product\n" +
		"  out:\n" +
		"    K8sNamespace: K8sNamespace\n" +
	    "  tagMaps:\n" +
	    "    Environment:\n" +
		"      maps:\n" + 
		"        Prod:\n" + 
		"          K8sNamespace: [ 're:.*prod.*', 're:.*production.*', 're:.*prd.*' ]\n" +
		"";
		
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		RuleConfig rc = new RuleConfig();
		rc = mapper.readValue(yaml, rc.getClass());

        String[] customTags = new String[]{"Cluster", "K8sNamespace"};
        ResourceService rs = new BasicResourceService(new BasicProductService(), customTags, false);
		KubernetesReport kr = new KubernetesReport(rc.getAllocation(), new DateTime("2020-08", DateTimeZone.UTC), rs);
		UserTag[] userTags = new UserTag[]{UserTag.get("k8s-ue1"), UserTag.empty};
		Set<String> names = kr.getClusterNameBuilder().getClusterNames(userTags);
		assertEquals("wrong number of cluster names", 1, names.size());
		assertEquals("wrong literal cluster name", "literal-cluster", names.iterator().next());
	}

}
