package com.epam.bdcc.bd101;

/**
 * Created by Volodymyr_Novostavsk on 11-Apr-17. BDCC course 101, homework #2.3
 * based on tutorial http://beekeeperdata.com/posts/hadoop/2015/07/26/Hive-UDTF-Tutorial.html
 **/

import com.epam.bdcc.bd101.UAgentAnalyzer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class UAgentAnalyzerTest {
    @Test
    public void testUAgent() {
        // set up the models we need
        UAgentAnalyzer agentAnalyzer = new UAgentAnalyzer();
        ObjectInspector[] inputOI = {PrimitiveObjectInspectorFactory.javaStringObjectInspector};

        // use actual agent string from the file bid.20131019.txt
        String name = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)";

        // the value exists
        try{
            agentAnalyzer.initialize(inputOI);
        }catch(Exception ex){
            ex.printStackTrace();
        }
        ArrayList<Object[]> results = agentAnalyzer.processInputRecord(name);

        //test output size
        Assert.assertEquals(1, results.size());
        //test exact values
        Assert.assertEquals("IE6", results.get(0)[0]);
        Assert.assertEquals("WINDOWS_XP", results.get(0)[1]);
        Assert.assertEquals("COMPUTER", results.get(0)[2]);
    }

}
