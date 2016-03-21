package org.apache.asterixdb.querygenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import datatype.Date;
import datatype.DateTime;
import socialGen.ADMAppendVisitor;
import socialGen.IAppendVisitor;
import socialGen.JsonAppendVisitor;

public class QGen {

    private final static int START_YEAR = 2000;
    private final static int START_MONTH = 1;
    private final static int START_DAY = 1;
    private final static int END_YEAR = 2014;
    private final static int END_MONTH = 8;
    private final static int END_DAY = 30;

    private final static Date startDate = new Date(START_MONTH, START_DAY, START_YEAR);
    private final static Date endDate = new Date(END_MONTH, END_DAY, END_YEAR);

    private static long[] RANGES = { 1000 * 60L, // minute
            1000 * 3600L, // hour
            1000 * 3600 * 24L, // day
            1000 * 3600 * 24 * 7L, // week
            1000 * 3600 * 24 * 30L, // month
            1000 * 3600 * 24 * 30L * 3L + 1 // 3 month
    };
    private static String[] RANGENAMES = { "minute", "hour", "day", "week", "month", "3-months" };

    private static IAppendVisitor admAppendVisitor = new ADMAppendVisitor() {

        @Override
        public IAppendVisitor visit(DateTime datetime) {
            builder.append("datetime(\'");
            datetime.accept(this);
            builder.append("\')");
            return this;
        }
    };
    private static IAppendVisitor jsonAppendVisitor = new JsonAppendVisitor();

    private static Map<String, Long> prefixToSeedMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            prefixToSeedMap.clear();
            File queries = new File("queries");
            FileUtils.deleteQuietly(queries);
            generateQueries(new File("templates"), queries);
        } else {
            int runs = Integer.parseInt(args[0]);
            for (int i = 0; i < runs; ++i) {
                prefixToSeedMap.clear();
                File queries = new File("queries" + i);
                FileUtils.deleteQuietly(queries);
                generateQueries(new File("templates"), queries);
            }
        }
    }

    private static void generateQueries(File src, File dest) throws Exception {
        if (src.isFile()) {
            return;
        }
        for (File child : src.listFiles()) {
            String lastName = child.getName();
            if (lastName.startsWith(".")) {
                continue;
            }
            if (child.isDirectory()) {
                File targetChild = new File(dest, lastName);
                FileUtils.forceMkdir(targetChild);
                generateQueries(child, targetChild);
            } else {
                for (int i = 0; i < RANGES.length; ++i) {
                    long range = RANGES[i];
                    String rangeStr = RANGENAMES[i];
                    String prefix = lastName.split("\\.")[0].split("_")[0] + rangeStr;
                    String generatedlastName = lastName.endsWith("sqlpp")
                            ? lastName.replaceAll("\\.sqlpp", "-" + rangeStr + "\\.sqlpp")
                            : lastName.replaceAll("\\.n1ql", "-" + rangeStr + "\\.n1ql");
                    if (!prefixToSeedMap.containsKey(prefix)) {
                        long seed = System.nanoTime();
                        prefixToSeedMap.put(prefix, seed);
                    }
                    long seed = prefixToSeedMap.get(prefix);
                    QueryDateTimeRandomGenerator generator = new QueryDateTimeRandomGenerator(startDate, endDate, seed);
                    File targetChild = new File(dest, generatedlastName);
                    targetChild.createNewFile();
                    DateTime startTime = generator.getNextRandomDatetime();
                    DateTime endTime = new DateTime();
                    endTime.reset(startTime.toTimestamp() + range);
                    generateQuery(child, targetChild, startTime, endTime);
                }
            }
        }
    }

    private static void generateQuery(File src, File dest, DateTime startTime, DateTime endTime) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(src)));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest)));
        StringBuilder templateQueryBuilder = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            templateQueryBuilder.append(line).append("\n");
        }
        reader.close();
        String templateQuery = templateQueryBuilder.toString();

        String startTimeStr = null;
        String endTimeStr = null;
        if (dest.getName().endsWith("sqlpp")) {
            startTimeStr = admAppendVisitor.reset().visit(startTime).toString();
            endTimeStr = admAppendVisitor.reset().visit(endTime).toString();
        } else {
            startTimeStr = jsonAppendVisitor.reset().visit(startTime).toString();
            endTimeStr = jsonAppendVisitor.reset().visit(endTime).toString();
        }
        String query = templateQuery.replace("$1", startTimeStr).replace("$2", endTimeStr);
        writer.write(query + "\n");
        writer.close();
    }

}
