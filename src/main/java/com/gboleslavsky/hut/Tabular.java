package com.gboleslavsky.hut;

import com.codepoetics.protonpack.*;

import java.util.*;
import java.util.stream.Stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static java.util.Arrays.asList;

public interface Tabular {
    /*  Mixin utility for String hiveTables
        some uses are:
         - in assert functionality to test any system that produces tabular data of actual vs a table with expected values.
            Expected data can be provided as Excel or as .csv. Column names are in the first line, and some way can be designed
            to define data types. Actual data can come from Hive or from a .csv file
         - to add convenience methods to any class that can implement Tabular. Functionality includes access by
            row or by column and comparison and diff of two Tabular instances of the same dimensions. */

//methods that need to be implemented  \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    int             numCols();
    int             numRows();
    String          val(int rowNum, int colNum);
    List<String>    headers();
//methods that need to be implemented  /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

    //TODO add column access by name and similar since headersAsRow are avail. Diffs can be used to find col name by index
    //which is enough to do any reporting
    //TODO write assert as jUnit 4 and check if that can be packaged to run as executable jar

//default methods >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    //types
    //if the data structure that implements Tabular is aware of column types
    //it should override this method to return those types, otherwise all values are
    //treated as STRING types
    default List<String> types() { return H.nOf("STRING", numCols()); }

    //row and column utilities
    default List<Integer> rowIndices(){ return H.indicesOneBased(numRows()); }
    default List<Integer> colIndices(){ return H.indicesOneBased(numCols()); }
    default List<String>  row(int rowNum){
        List<Integer> colIndices = colIndices();
        List<String> rvs = new ArrayList<>();
        for (int ci : colIndices) rvs.add(val(rowNum, ci));
        return rvs;
    }
    default String rowValue(String colNameProvided, String colValueProvided, String colNameNeeded) {
        int rowIndex = H.upper(col(colIndex(colNameProvided))).indexOf(H.inQuotes(colValueProvided));
        return rowIndex == -1 ? "" : val(rowIndex+1, colIndex(colNameNeeded));
    }

    default List<String> rowValues(String colNameProvided, String colValueProvided, String colNameNeeded) {
        int rowIndex = H.upper(col(colIndex(colNameProvided))).indexOf(H.inQuotes(colValueProvided.toUpperCase()));
        return colSlice(colIndex(colNameNeeded), rowIndex+1);
    }

    default List<List<String>> rows() {
        List<Integer> rowIndices = rowIndices();
        List<List<String>> rws = new ArrayList<>();
        for (int ri : rowIndices) rws.add(row(ri));
        return rws;
    }

    default List<List<String>> sortedRows() {
        List<Integer> rowIndices = rowIndices();
        List<List<String>> rws = new ArrayList<>();
        for (int ri : rowIndices) rws.add(row(ri));
        return rws;
    }

    default Stream<List<String>> rs() { return rows().stream(); }

    default List<String> colSlice(int colNum, int rowStart, int rowEnd) {
        List<String> cvs = new ArrayList<>();
        for (int ri=rowStart; ri<=rowEnd; ri++ ) cvs.add(val(ri, colNum));
        return cvs;
    }

    default List<String> colSlice(int colNum, int rowStart) {
        return colSlice(colNum,  rowStart, rowIndices().size());
    }

    default List<String> col(int colNum) {
        List<Integer> rowIndices = rowIndices();
        List<String> cvs = new ArrayList<>();
        for (int ri : rowIndices) cvs.add(val(ri, colNum));
        return cvs;
    }
    default List<List<String>> cols() {
        List<Integer> colIndices = colIndices();
        List<List<String>> cs = new ArrayList<>();
        for (int ci : colIndices) cs.add(col(ci));
        return cs;
    }

    default int colIndex(String colName) { return Math.max( headers().indexOf(H.inQuotes(colName)),
                                                            headers().indexOf(H.noQuotes(colName))) + 1;}

    //diffs
    static Tabular csvDiffs(String file1Contents, String file2Contents)   { return tabularCsv(file1Contents)
                                                                                    .diffs(tabularCsv(file2Contents)); }
    
    //BiFunction is needed to be able to zip 2 streams l and r by applying it to (l,r) and
    //producing a list of results. see Tabular.diffs()
    static BiFunction<List<String>, List<String>, List<String>> biDiffs() { return (l, r) -> diffs(l, r); }

    static Tabular diffs(Tabular l, Tabular r)      {
        List<List<String>> lRows =  l.rows();
        List<List<String>> rRows =  r.rows();
        return tabularStrings( H.list 
                                (StreamUtils.zip
                                (lRows.stream(), rRows.stream(), biDiffs()) ));
    }

    default Tabular diffs(Tabular t)                { return diffs(this, t);}

    default boolean isEmpty()                       { return rs().  map(r -> H.isEmpty(r)).
                                                                    reduce((r1, r2 )-> r1 && r2).
                                                                    orElse(false); }

    default String  strHeaders()                    { return H.join(H.noQuotes(headers()), " | ");}

    default String  str()                           { return H.separateLines( H.list(rows().stream().map(r -> H.join(r, " | "))));}

    default void diffsToFile(String path, String fileName, Tabular l, Tabular r){
        H.saveAsTextFile(path, fileName, l.strHeaders() + "\n" + diffs(l, r).str());
    }

    default void toFile(String path, String fileName){
        H.saveAsTextFile(path, fileName, strHeaders() + "\n" + str());
    }

    default List<List<String>> toStringLists(){
        List<List<String>> lists = new ArrayList<>();
        lists.add(headers());
        lists.addAll(rows());
        return lists;
    }

    //arithmetic
     default Tabular plus(Tabular t2){
         List<List<String>> intermediate = rows();
         intermediate.addAll(t2.rows());
         List<List<String>> sum = new ArrayList<>();
         sum.add(headers());
         sum.addAll(intermediate);
         return tabularStrings(sum);
     }

    default boolean equal(Tabular t)                { return diffs(t).isEmpty();}

    //factory methods
    static Tabular tabularStrings(List<List<String>> lists){
        return new Tabular(){
            public int          numRows()                   { return lists.size()-1; }
            public int          numCols()                   { return lists.get(0).size(); }
            public String       val(int rowNum, int colNum) { return lists.get(rowNum).get(colNum-1); }
            public List<String> headers()                   { return lists.get(0); }
        };
    }

    default Tabular empty(){
        List<String> e = H.nOf("", numCols());
        return tabularStrings(Arrays.asList(headers(), e));
    }

    static Tabular tabularCsv(String path)                 { return tabularStringArrays(H.csvData(path));}

    static Tabular tabularStringArrays(List<String[]> lists) {
        List<List<String>> ls = new ArrayList<>();
        for (String[] a:lists) ls.add(asList(a));
        return tabularStrings(ls);
    }

    static Tabular tabularDataset(Dataset<Row> ds){
        return new Tabular(){
            public int          numRows()                   { return (int)ds.count(); }
            public int          numCols()                   { return ds.columns().length; }
            public List<String> headers()                   { return Arrays.asList(ds.columns()) ; }
            public String val(int rowNum, int colNum) {
                int ri = rowNum-1;
                int ci = colNum-1;
                Object v = ds.collectAsList().get(ri).get(ci);
                return v == null ? "" : v.toString(); }
        };
    }
}

