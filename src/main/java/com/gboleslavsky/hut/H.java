package com.gboleslavsky.hut;

import com.beust.jcommander.internal.Lists;
import com.univocity.parsers.common.processor.RowListProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.text.ParseException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.gboleslavsky.hut.Hql.*;
import static com.gboleslavsky.hut.Hql.Parser.hiveTables;
import static java.util.Arrays.asList;

//Helper functions
public class H {

    // ------------------------------- stream utilities -------------------------------
    static <T> String join(List<T> l) {
        return join(l, "");
    }

    static <T> String join(List<T> l, String separator) {
        if (l == null) return "";
        StringBuilder b = new StringBuilder();
        l.stream().map(o -> wrap(o + separator, "")).forEach(b::append);
        String strValues = b.toString();
        if (separator.length() > 0 && strValues.lastIndexOf(separator)>0)//drop the last separator
            strValues = strValues.substring(0, strValues.lastIndexOf(separator));
        return strValues;
    }

    static <T> List<T> list(Stream<T> s) {
        return s.collect(Collectors.toList());
    }

    // ------------------------------- List utilities -------------------------------
    static List<String> zip(List<String> l1, String separator, List<String> l2) {
        List<String> zipped = new ArrayList<>();
        if (l1==null || l2 == null) return zipped;
        Iterator<String> i1 = l1.iterator();
        Iterator<String> i2 = l2.iterator();
        while (i1.hasNext() && i2.hasNext()) {
            String iOne = i1.next();
            iOne = iOne == null ? " " : iOne;
            String iTwo = i2.next();
            iTwo = iTwo == null ? " " : iTwo;
            zipped.add(iOne +separator + iTwo);
        }
        return zipped;
    }

    static List<String> listJoin(List<String> l, String separator){
        List<String> joined = H.list(l.stream().map(o -> wrap(o + separator, "")));
        joined.remove(joined.size()-1);
        return joined;
    }

    static boolean nullOrEmpty(List<String> l) {return l == null || l.size() == 0;}

    static List<Integer> indices(int upToExclusive) { return H.list(IntStream.range(0, upToExclusive).boxed()); }
    static List<Integer> indicesOneBased(int upToExclusive) { return H.list(IntStream.rangeClosed(1, upToExclusive).boxed()); }

    static List<String> upper(List<String> l) { return H.list(l.stream().map(s-> s==null ? "" : s.toUpperCase())); }

    static List<String> noQuotes(List<String> l) { return H.list(l.stream().map(s->noQuotes(s))); }

    static String[][] asArray(List<List<String>> m) {
        return m.stream()
            .map(l -> l.stream().toArray(String[]::new))
            .toArray(String[][]::new);
    }

    // ------------------------------- String utilities -------------------------------
    static String surround(String pref, String s, String postf)     { return pref + s + postf; }
    static String wrap(String s, String w)                          { return surround(w, s, w); }
    static String inQuotes(String s)                                { return wrap(s, "'"); }
    static String paren(String s)                                   { return surround("(", s, ")");}
    static String separateLines(List<String> l)                     { return H.join(l, "\n\n"); }
    static String commaSeparateLines(List<String> l)                { return H.join(l, ",\n"); }
    static String semicolonSeparateLines(List<String> l)            { return H.join(l, ";\n"); }
    static String noQuotes(String s)                                { return s==null ? s : s.replaceAll("'", "");}

    static List<String> nOf(String oneOf, int n){
        String[] allN = new String[n];
        Arrays.fill(allN, oneOf);
        return asList (allN);
    }

    // ------------------------------- type conversion utilities -------------------------------
    static boolean isInt(double d)                                   { return d == Math.rint(d); }
    static String  hiveNumType(double d)                             { return isInt(d) ? HIVE_INT_TYPE : HIVE_DBL_TYPE; }
    static String  hiveDate(Date d)                                  { return inQuotes(HIVE_DATE_FORMAT.format(d)); }

    static String hiveTimestamp(String t) {
        try {
            return inQuotes(HIVE_TIMESTAMP_FORMAT.format(EXCEL_TIMESTAMP_FORMAT.parse(t)));
        } catch (Exception e) {
            return "";
        }
    }

    static boolean isTimeStamp(String s) {
        try {
            EXCEL_TIMESTAMP_FORMAT.parse(s);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    // ------------------------------- CSV parser utilities -------------------------------
    static RowListProcessor csvParser(String path, boolean extractHeader) {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.setLineSeparatorDetectionEnabled(true);
        RowListProcessor rowProcessor = new RowListProcessor();
        parserSettings.setRowProcessor(rowProcessor);
        parserSettings.setHeaderExtractionEnabled(extractHeader);
        new CsvParser(parserSettings).parse(new File(path));
        return rowProcessor;
    }

    static List<String[]> csvData(String path) {
        return csvParser(path, false).getRows();
    }

    static List<String> csvHeaders(String path) {
        return Arrays.asList(csvParser(path, true).getHeaders());
    }
    // ------------------------------- diff utilities -------------------------------
    static boolean isEmpty(List<String> results) {
        return list(
                results.stream().
                        filter(r -> r.length() > 0)).
                size() == 0;
    }

    static boolean emptyValue(String v){
        return  Arrays.asList("null", "-99999", null, "", "'null'").contains(v);
    }

    // ------------------------------- File utilities -------------------------------
    static Properties loadConfig(String confPath) {
        Properties conf = new Properties();
        try { try(FileInputStream cf = new FileInputStream(new File(confPath ))){ conf.load(cf); } }
        catch (IOException e) { e.printStackTrace(); }
        return conf;
    }

    static String confValue(Properties conf, String key)                { return (String) conf.get(key);}
    static String confValue(String confPath, String key)                { return (String) loadConfig(confPath).get(key);}
    static String currentDirectory()                                    { return System.getProperty("user.dir") + "/"; }
    static String inputDirectory(String confPath)                       { return confValue(confPath, "inDir") + "/"; }
    static String blScriptsDirectory(String subDir)                     { return currentDirectory() + subDir + "/"; }
    static String hqlDirectory(String confPath)                         { return confValue(confPath, "hqlDir") + "/"; }
    static String hqlDirectory    (String confPath, String modelName)   { return hqlDirectory(confPath) + modelName +"/"; }
    static String createsDirectory(String confPath, String modelName)   { return hqlDirectory(confPath, modelName) + "creates/"; }
    static String insertsDirectory(String confPath, String modelName)   { return hqlDirectory(confPath, modelName) + "inserts/"; }
    static String selectsDirectory(String confPath, String modelName)   { return hqlDirectory(confPath, modelName) + "selects/"; }
    static String deletesDirectory(String confPath, String modelName)   { return hqlDirectory(confPath, modelName) + "deletes/"; }
    static String dropsDirectory  (String confPath, String modelName)   { return hqlDirectory(confPath, modelName) + "drops/"; }
    static String truncatesDirectory(String confPath, String modelName) { return hqlDirectory(confPath, modelName) + "truncates/"; }
    static String resultsDirectory()                                    { return currentDirectory() + "results/"; }
    static String resultsDirectory(String modelName)                    { return resultsDirectory() + modelName +"/"; }

    static void saveAsTextFile(String path, String fileName, String text){
        // Files.newBufferedWriter() uses UTF-8 encoding by default
        // the file will be automatically closed since it uses try context manager -- ry (BufferedWriter writer = ...
        try {
            Files.createDirectories(Paths.get(path));
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path + fileName))) { writer.write(text);}
        }
        catch (IOException e) { e.printStackTrace(); }
    }

    static String fileToString(String p) {return fileToString(Paths.get(p));}
    static String fileToString(Path p) {
        try { return new String(Files.readAllBytes(p), Charset.forName("UTF-8"));}
        catch (IOException e) { e.printStackTrace(); return ""; }
    }

    static List<Path> files(String absolutePathToDirectory){
        List<Path> fs = Lists.newArrayList();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(absolutePathToDirectory))) {
            for (Path file: ds) fs.add(file);
        } catch (IOException | DirectoryIteratorException x) {
            // IOException can never be thrown by the iteration, only by newDirectoryStream.
            System.err.println(x);
        }
        return fs;
    }

    static List<Path> excelFiles(String confPath) {
        return H.list(files(inputDirectory(confPath)).stream().filter(f->isExcelFile(f)));
    }
    static List<Path> hqlFiles(String p) { return hqlFiles(Paths.get(p)); }
    static List<Path> hqlFiles(Path p) {
        return H.list(files(p.toString()).stream().filter(f->isHqlFile(f)));
    }

    static boolean hasExtension(Path f, String ext) {return extension(f).equals(ext);}

    static boolean isExcelFile(Path f) {return hasExtension(f, "xlsx");}
    static boolean isHqlFile(Path f) {return hasExtension(f, "hql");}

    static String extension(Path f) {
        String fn = f.getFileName().toString();
        int dotIndex = fn.lastIndexOf(".");
        return fn.substring(dotIndex + 1);
    }

    static String modelName(Path p){
        String fn = p.getFileName().toString();
        int dotIndex = fn.lastIndexOf(".");
        return fn.substring(0, dotIndex);
    }

    static void createModelDirectories(String confPath, String modelName) {
        try {
            //Files.createDirectories(Paths.get( inputDirectory()));
            Files.createDirectories(Paths.get( createsDirectory(confPath, modelName)));
            Files.createDirectories(Paths.get( insertsDirectory(confPath, modelName)));
            Files.createDirectories(Paths.get( selectsDirectory(confPath, modelName)));
            Files.createDirectories(Paths.get( resultsDirectory(modelName)));
            Files.createDirectories(Paths.get( dropsDirectory(confPath, modelName)));
            Files.createDirectories(Paths.get( deletesDirectory(confPath, modelName)));
            Files.createDirectories(Paths.get( truncatesDirectory(confPath, modelName)));
        } catch (IOException e) {
            System.out.println("File system problems with folder creation for model " + modelName);
            e.printStackTrace();
        }
    }

    static List<String> hqlModels(String confPath) throws IOException{
        return H.list(Files.list(Paths.get(hqlDirectory(confPath))).filter(Files :: isDirectory).map(p->lastDirectory(p)));
    }

    static String lastDirectory(Path p){
        String[] allDirs = p.toString().replace("\\", "/").split("/");
        return allDirs[allDirs.length-1];
    }

    // -------------- String hack to simulate a tuple for result of comparison --------------
    static String result(String left, String right) {
        return left + "~" + right;
    }


    static String left(String result) {
        String[] afterSplit = result.split("~");
        if (afterSplit.length >=2) return afterSplit[0];
        if (result.charAt(0) == '~') {
            return afterSplit[0];
        } else {
            return "";
        }
    }

    static String right(String result) {
        String[] afterSplit = result.split("~");
        if (afterSplit.length >=2) return afterSplit[1];
        if (result.charAt(result.length()-1) == '~') {
            return afterSplit[0];
        } else {
            return "";
        }
    }


    //--------------------------------------------------------------------------------------------------------
    //various functions for coarse units of work to make the top level overall process steps dead simple and obvious
    //used mainly by CommandInterpreter Command classes

    public static void excelToHql(String confPath){
        List<Path> inFiles = H.excelFiles(confPath);
        inFiles
                .forEach(f-> H.createModelDirectories(confPath, H.modelName(f)));
        inFiles
                .forEach(f->hiveTables(f, confPath)
                        .forEach(t->t.saveHqlToFile()));
    }

    public static String hqlCreateScript(String confPath, String modelName){
        List<String> createScripts = new ArrayList<>();
        for (Path p : hqlFiles(createsDirectory(confPath, modelName))){
            createScripts.add(fileToString(p));
        }
        return join(createScripts,"\n");
    }

    public static String hqlDeleteScript(String confPath, String modelName){
        List<String> deletesScripts = new ArrayList<>();
        for (Path p : hqlFiles(deletesDirectory(confPath, modelName))){
            deletesScripts.add(fileToString(p));
        }
        return join(deletesScripts,"\n");
    }

    public static String hqlInsertScript(String confPath, String modelName){
        List<String> insertScripts = new ArrayList<>();
        for (Path p : hqlFiles(insertsDirectory(confPath, modelName))){
            insertScripts.add(fileToString(p));
        }
        return join(insertScripts,"\n");
    }

    public static String hqlTruncateScript(String confPath, String modelName){
        List<String> truncateScripts = new ArrayList<>();
        for (Path p : hqlFiles(truncatesDirectory(confPath, modelName))){
            truncateScripts.add(fileToString(p));
        }
        return join(truncateScripts,"\n");
    }

    public static void loadHqlData(String confPath) throws IOException{
        for(String modelName: hqlModels(confPath)){
            LocalSparkHqlRunner.runScript(hqlCreateScript(confPath, modelName));
            LocalSparkHqlRunner.runScript(hqlInsertScript(confPath, modelName));
        }
    }

    public static void removeHqlData(String confPath) throws IOException{
        for(String modelName: hqlModels(confPath)){
            LocalSparkHqlRunner.runScript(hqlTruncateScript(confPath, modelName));
        }
    }

    public static String schemaName(Tabular conf, String tableName){
        return noQuotes(conf.rowValue("property", tableName.toUpperCase(), "value"));
    }

    public static List<String> keyColumns(Tabular conf, String tableName){
        return Arrays.asList(noQuotes(conf.rowValue("property", "key_columns", "value")).split(","));
    }

    public static List<String> scriptsToRun(Tabular conf, String scriptFolder){
        List<String> scriptNames = H.list(conf.rowValues("conf_type", "business_logic", "value")
                                        .stream().map(sN->noQuotes(sN)));
        return H.list(scriptNames.stream().map(s->blScriptsDirectory(scriptFolder)  + s));
    }

    public static List<String> scriptsToRun(Tabular excelConf){
        return scriptsToRun(excelConf, "bl_scripts");
    }

    public static List<String> scriptsToRun(String confPath){
        List<Tabular> confs = H.list(H.excelFiles(confPath).stream().map(f->Parser.excelConf(f, confPath)));
        List<String> scripts = new ArrayList<>();
        for(Tabular c: confs) {
            scripts.addAll(scriptsToRun(c));
        }
        return scripts;
    }

    public static Tabular actual(HiveTable expected){
        List<String> selects = expected.selectsWithWheres();
        Tabular sum = Tabular.tabularDataset(LocalSparkHqlRunner.runStatementReturnResults(selects.get(0)));
        for (int i = 1; i<selects.size();  i++){
            Tabular result = Tabular.tabularDataset(LocalSparkHqlRunner.runStatementReturnResults(selects.get(i)));
            if (result.numRows() == 0)
                result = result.empty();
            sum = sum.plus(result);
        }
        return sum;
    }

}
