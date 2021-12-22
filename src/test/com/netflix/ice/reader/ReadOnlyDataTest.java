package com.netflix.ice.reader;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.netflix.ice.common.TimeSeriesData;
import com.netflix.ice.tag.CostType;
import com.netflix.ice.tag.TagType;
import com.netflix.ice.tag.UserTag;
import org.apache.commons.lang.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.ice.basic.BasicAccountService;
import com.netflix.ice.basic.BasicProductService;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import com.netflix.ice.tag.Zone.BadZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReadOnlyDataTest {
    protected Logger logger = LoggerFactory.getLogger(getClass());
	private static final String dataDir = "src/test/resources/private/";
    
    private static AccountService as;
    private static ProductService ps;
    
	@BeforeClass
	public static void init() throws IOException {
		as = new BasicAccountService();
        ps = new BasicProductService();
	}
	

	@Test
	public void testFileRead() throws IOException, BadZone {
        String filename = "cost_daily_EC2_2020";
        boolean forReservations = false;
        int numUserTags = 16;
       
        File file = new File(dataDir, filename + ".gz");
        
        if (!file.exists()) {
        	// Don't run the test if the file doesn't exist
        	return;
        }
    	InputStream is = new FileInputStream(file);
    	is = new GZIPInputStream(is);
        DataInputStream in = new DataInputStream(is);
        ReadOnlyData data = new ReadOnlyData(numUserTags);
        
        try {
            data.deserialize(as, ps, in, forReservations);
        }
        finally {
            if (in != null)
                in.close();
        }

		Collection<TagGroup> tagGroups = data.getTagGroups(null, null, 0);
		assertNotNull("tagGroups is NULL", tagGroups);

		tagGroups = data.getTagGroups(TagType.CostType, CostType.recurring, 0);
		assertNotNull("Cost type tagGroups for recurring is NULL", tagGroups);

		tagGroups = data.getTagGroups(TagType.Tag, UserTag.get("Dev"), 9);
		assertNotNull("User Tag tagGroups index for Environment is NULL", tagGroups);

        dump(data);
	}

	private void dump(ReadOnlyData data) {
		for (TagGroup tg: data.getTagGroups(null, null, 0)) {
			TimeSeriesData tsd = data.getData(tg);
			double[] values = new double[tsd.size()];
			tsd.get(TimeSeriesData.Type.COST, 0, tsd.size(), values);
			Double[] doubleArray = ArrayUtils.toObject(values);
			List<Double> d = Arrays.asList(doubleArray);
			logger.info("  " + tg + ": " + d);
		}
	}

	@Test
	public void testTimeSeriesData() {
		double[] cost  = new double[]{ 0, 1, 0 };
		double[] usage = new double[]{ 0, 1, 0 };

		TimeSeriesData tsd = new TimeSeriesData(cost, usage);

		// Test pulling the full array
		double[] got = new double[3];
		tsd.get(TimeSeriesData.Type.COST, 0, 3, got);
		for (int i = 0; i < cost.length; i++) {
			assertEquals("cost doesn't match at index " + i, cost[i], got[i], 0.00001);
		}

		// Test pulling at start of 1
		int offset = 1;
		got = new double[cost.length - offset];
		tsd.get(TimeSeriesData.Type.COST, offset, cost.length - offset, got);
		for (int i = 0; i < got.length; i++) {
			assertEquals("cost doesn't match at index " + i, cost[i+offset], got[i], 0.00001);
		}
	}

}
