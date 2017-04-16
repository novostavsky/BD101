package com.epam.bdcc.bd101;

/**
 * Created by Volodymyr_Novostavsk on 11-Apr-17. BDCC course 101, homework #2.3
 * based on tutorial http://beekeeperdata.com/posts/hadoop/2015/07/26/Hive-UDTF-Tutorial.html
 **/

import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Description(
        name = "uaparse",
        value = "_FUNC_(str) - Parses a userAgent string and gets browser, os and device",
        extended = "Example:\n" +
                "  > SELECT t.browser, t.os, t.device \n" +
                "    FROM Table LATERAL VIEW uaparse(userAgent) t as browser, os, device;\n" +
                "IE WINDOWS_XP  COMPUTER"
)
@UDFType(deterministic = false)
public class UAgentAnalyzer extends GenericUDTF{
    private PrimitiveObjectInspector stringOI = null;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        if (args.length != 1) {
            throw new UDFArgumentException("com.epam.bdcc.bd101.UAgentAnalyzer() takes exactly one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("com.epam.bdcc.bd101.UAgentAnalyzer() takes a string as a parameter");
        }

        // input inspectors
        stringOI = (PrimitiveObjectInspector) args[0];

        // output inspectors -- an object with 3 fields!
        List<String> fieldNames = new ArrayList<String>(3);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(3);

        //add fields
        fieldNames.add("browser");
        fieldNames.add("os");
        fieldNames.add("device");

        //add inspectors
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public ArrayList<Object[]> processInputRecord(String inRecord){
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        // ignoring null or empty input
        if ((inRecord == null) || (inRecord.isEmpty() || (inRecord.equals(new String(" "))))) {
            return result;
        }

        //get meaningful info from user agent string
        UserAgent userAgent = UserAgent.parseUserAgentString(inRecord);
        String browser = userAgent.getBrowser().toString();
        String os = userAgent.getOperatingSystem().toString();
        String device = userAgent.getOperatingSystem().getDeviceType().toString();

        //add info to result array and return the array
        result.add(new Object[] { browser, os, device });
        return result;
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final String strToProcess = stringOI.getPrimitiveJavaObject(record[0]).toString();

        ArrayList<Object[]> results = processInputRecord(strToProcess);

        Iterator<Object[]> it = results.iterator();

        while (it.hasNext()){
            Object[] r = it.next();
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }

}
