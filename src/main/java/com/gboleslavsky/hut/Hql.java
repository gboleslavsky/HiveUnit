package com.gboleslavsky.hut;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Hql {
    //excel to hiveQL utilities singleton with static methods and classes
    private Hql() {}

    private static String HIVE_STRING_TYPE      = "STRING";
    private static String HIVE_TIMESTAMP_TYPE   = "TIMESTAMP";
    private static String HIVE_DATE_TYPE        = "DATE";
            static String HIVE_INT_TYPE         = "INT";
            static String HIVE_DBL_TYPE         = "DOUBLE";
    private static String HIVE_BLANK_TYPE       = "BLANK";
    private static String HIVE_BLANK_VALUE      = "null";

    static SimpleDateFormat EXCEL_TIMESTAMP_FORMAT  =    new SimpleDateFormat("MM/dd/yyyy  hh:mm:ss aa");
    static SimpleDateFormat HIVE_TIMESTAMP_FORMAT  =     new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat HIVE_DATE_FORMAT =           new SimpleDateFormat("yyyy-MM-dd");

    public static class Parser {
        private Parser() {}

        public static Workbook workbook(Path path) throws IOException { return new XSSFWorkbook(new FileInputStream(path.toString())); }

        public static HiveTable excelConf(Path path, String confPath){
            try {
                return new HiveTable(sheets(workbook(path)).get(0), confPath);
            }
            catch(IOException e){
                e.printStackTrace();
            }
            return new HiveTable();
        }

        public static List<HiveTable> hiveTables(Path path, String confPath) {
            try {
                List<HiveTable> hiveTables = H.list(sheets(workbook(path)).stream().map(s->new HiveTable(s, confPath)));
                HiveTable conf = hiveTables.get(0);
                hiveTables.remove(0);
                hiveTables.forEach(t->t.schemaName= H.schemaName(conf, t.name));
                hiveTables.forEach(t->t.modelName = H.modelName(path));
                hiveTables.forEach(t->   t.tabConf =conf);
                return hiveTables;
            } catch (Exception e) {
                //System.out.println("Bad file: " + path);
                e.printStackTrace();
            }
            return new ArrayList<>();
        }

        public static List<Sheet> sheets(Workbook wb) {
            ArrayList<Sheet> sL = new ArrayList<>();
            for (Sheet sheet : wb) sL.add(sheet);
            return sL;
        }

        private static boolean justCharsAndUnderscore(String s) { return s.matches("^[A-Z0-9_-]*$"); }
        static boolean isHiveTable(Sheet s)                     { return isInputTable(s) || isExpectedTable(s); }
        static boolean isInputTable(Sheet s)                    { return isInputTable(s.getSheetName().toUpperCase()); }
        static boolean isInputTable(String sN) {                  return justCharsAndUnderscore(sN)
                                                                         &&
                                                                         ((sN.endsWith("_IN") || sN.startsWith("IN_")));}
        static boolean isExpectedTable(Sheet s) {                 return isExpectedTable(s.getSheetName().toUpperCase());}
        static boolean isExpectedTable(String sN) {               return justCharsAndUnderscore(sN)
                                                                         &&
                                                                         ((sN.endsWith("_EXP") || sN.startsWith("EXP_")));}

        static List<Sheet> hiveTables(List<Sheet> sL)   { return H.list(sL.stream().filter(s -> isHiveTable(s))); }
        static List<Sheet> hiveTables(Workbook wb)      { return hiveTables(sheets(wb)); }

        static List<org.apache.poi.ss.usermodel.Row> rows(Sheet sheet) {
            ArrayList<org.apache.poi.ss.usermodel.Row> rL = new ArrayList<>();
            for ( org.apache.poi.ss.usermodel.Row r : sheet) rL.add(r);
            return rL;
        }

        static List<Cell> cells(org.apache.poi.ss.usermodel.Row row) {
            ArrayList<Cell> cL = new ArrayList<>();
            for (Cell c : row) cL.add(c);
            return cL;
        }

        static Cell firstNonBlank(List<Cell> cells){
            if (cells == null) return null;
            return cells.stream().filter(c -> c != null && !cellType(c).equals(HIVE_BLANK_TYPE)).findFirst().orElse(null); }

        static String colType(List<Cell> column) { return cellType(firstNonBlank(column)); }

        static String cellType(Cell c) {
            if (c == null) return HIVE_STRING_TYPE;
            CellType cT = c.getCellTypeEnum();
            if (cT == CellType.STRING){
                if (H.isTimeStamp(c.getRichStringCellValue().getString()))
                    return HIVE_TIMESTAMP_TYPE;
                else
                    return HIVE_STRING_TYPE ;
            }
            if (cT == CellType.BLANK) return HIVE_BLANK_TYPE;
            if (cT == CellType.NUMERIC) {
                if (DateUtil.isCellDateFormatted(c)) {
                    return HIVE_DATE_TYPE;
                } else {
                    return H.hiveNumType(c.getNumericCellValue());
                }
            }
            if (cT == CellType.FORMULA){
                try {
                    return H.hiveNumType(c.getNumericCellValue());
                }catch (IllegalStateException e){
                   return HIVE_STRING_TYPE;
                }
            }
            return cT.toString();
        }

        static String cellValue(Cell c) {
            return cellValue(c, cellType(c)); }

        static String cellValue(Cell c, String type) {
            if (type == HIVE_BLANK_TYPE)     return HIVE_BLANK_VALUE;
            if (type == HIVE_DATE_TYPE)      return H.hiveDate(c.getDateCellValue());
            if (type == HIVE_DBL_TYPE)       return String.valueOf(c.getNumericCellValue());
            if (type == HIVE_INT_TYPE)       return String.valueOf((int) c.getNumericCellValue());
            if (type == HIVE_STRING_TYPE)    return H.wrap(c.getRichStringCellValue().getString(), "'");
            if (type == HIVE_TIMESTAMP_TYPE) return H.hiveTimestamp(c.getRichStringCellValue().getString());
            return c.getRichStringCellValue().getString();
        }
    }

    // Data Model classes ----------------------------------------------------------------------------------------------
     public static class HiveCell{
        Cell   exCell;
        String hiveValue;
        String hiveType;

         HiveCell(){}

         HiveCell(Cell cell){
            exCell =    cell;
            hiveValue = Parser.cellValue(cell); //sometimes unreliable on cell level
            hiveType =  Parser.cellType(cell);  //sometimes unreliable on cell level
        }

         void reset(String type){
            //when a cell in a column is blank, the type gets set incorrectly
            //and gets reset when all columns are transformed and there is at least
            //one non-null value so cell cab be interrogated for type
            hiveType = type;
            hiveValue = Parser.cellValue(exCell, type);
        }

         int rowNum(){ return exCell.getRowIndex(); }

         int colNum(){ return exCell.getColumnIndex() +1; }

         public String toString() { return hiveValue+" "+hiveType+" "+" ("+rowNum()+ ", "+ colNum() +") "; }
    }

    public static class HiveRow {
        List<HiveCell> cells;
        int rowNum;

         HiveRow() { cells = new ArrayList<>(); rowNum = -1; }

         HiveRow(org.apache.poi.ss.usermodel.Row excelRow) {
            cells = H.list(Parser.cells(excelRow).stream().map(HiveCell::new));
            rowNum = cells.stream().findFirst().map(c -> c.rowNum()).orElse(-1);
        }

         HiveCell colValue(int colNum) {
            return cells.stream().filter(v -> v.colNum() == colNum).findFirst().orElse(new HiveCell());
        }

         List<String> hiveValues()    { return H.list(cells.stream().map(c -> c.hiveValue)); }
         List<String> createValues()  { return H.list(hiveValues().stream().map(s -> H.noQuotes(s))); }
         String       insertValue()   { return H.paren(H.join(hiveValues(), ",")); }

         String colNameEqualsValuesForWhere(List<String> colNames) {
             colNames =  H.list( colNames.stream().map(cn-> H.noQuotes(cn)));
             List<String> nameValues = H.zip(colNames , "~", hiveValuesAsStringList());
             List<String> nonEmptyNameValues = H.list(nameValues.stream().filter(r->!H.emptyValue((H.right(r)))));
             nonEmptyNameValues = H.list(nonEmptyNameValues.stream().map(r->{r=r.replace("~"," = "); return r;}));
             return H.join(nonEmptyNameValues, " AND\n");
         }

         List<String> types() {
            return H.list(cells.stream().map(c -> c.hiveType).map(s -> (s.equals(HIVE_BLANK_TYPE)) ? "STRING" : s));
        }

         List<String> hiveValuesAsStringList() {
             return H.list(
                        cells.stream().
                        map(c -> c.hiveValue));
                        //filter(v->nonEmpty(v)));
         }

         public String toString() { return H.join(H.list(cells.stream().map(c -> c.toString()))); }
    }

    public static class HiveTable implements Tabular {
         String confPath;
         String name;
         String excelName;
         String modelName;
         String schemaName;
         HiveTable tabConf;
         List<HiveRow> rows;

         public HiveTable() {}

         public HiveTable(Sheet sheet, String confP) {
             confPath = confP;
             excelName = sheet.getSheetName().toUpperCase();
             name = excelName.   replace("_IN" , "").
                                 replace("IN_" , "").
                                 replace("_EXP", "").
                                 replace("EXP_", "");
             rows = H.list(Parser.rows(sheet).stream().map(HiveRow::new));
         }

         public String withSchema(){
             return schemaName==null || schemaName.length()==0 ? name : schemaName + "." + name;
         }

         //Tabular implementation \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
         public int numRows() {
             return rows.size()-1;
         }

         public int numCols() {
             return columns().size();
         }

         public String val(int rowNum, int colNum) {
             return rows.get(rowNum ).colValue(colNum).hiveValue;
         }

         public List<String> headers() { return H.noQuotes(headersAsRow().hiveValuesAsStringList()); }
         //toFile needs to be different since HiveTable's headers are in row(0) and would otherwise be saved twice
         //public void toFile(String path, String fileName){ H.saveAsTextFile(path, fileName,  str());}
         //Tabular implementation /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

         public List<String> types() {
             return numRows() > 0 ? rows.get(0).types() : Tabular.super.types();
         }

         String commaSeparatedColumnNames() {
             return H.noQuotes(H.join(headers(), ","));
         }

         HiveRow headersAsRow() {
             return rows.stream().findFirst().map(r -> r).orElse(new HiveRow());
         }

         List<HiveRow> data() {
             return H.list(rows.stream().skip(1));
         }

         Stream<HiveRow> ds() {
             return data().stream();
         }

         HiveRow hiveRow(int rowNum) {
             return ds().filter(r -> r.rowNum == rowNum).findFirst().orElse(new HiveRow());
         }

         Stream<HiveCell> hiveCs(int colNum) {
             return ds().map(r -> r.colValue(colNum));
         }

         List<HiveCell> hiveCol(int colNum) {
             return H.list(hiveCs(colNum));
         }

         List<String> hiveColValues(int colNum) {
             return H.list(hiveCs(colNum).map(c -> c.toString()));
         }

         List<List<HiveCell>> columns() {
             List<Integer> indices = H.list(IntStream.rangeClosed(1, headersAsRow().cells.size()).boxed());
             List<List<HiveCell>> cs = new ArrayList<>();
             for (int i : indices) cs.add(hiveCol(i));
             return cs;
         }

         private static List<Cell> exCells(List<HiveCell> column) {
             return H.list(column.stream().map(c -> c.exCell));
         }

         private List<String> columnTypes() {
             List<String> cts = new ArrayList<>();
             for (List<HiveCell> col : columns()) {
                 cts.add(Parser.colType(exCells(col)));
             }
             return cts;
         }

         private String strColumnValues() {
             List<Integer> indices = H.list(IntStream.rangeClosed(1, headersAsRow().cells.size()).boxed());
             List<String> strColVals = new ArrayList<>();
             for (int i : indices) strColVals.add(H.join(hiveColValues(i)));
             return H.join(strColVals, "\n");
         }

         //private void setTypes() {
         //   columns().stream().forEach(column -> setType(H.list(column.stream().map(c -> c.exCell)), )
         //}
         private void setType(List<HiveCell> column, String type) {
             setType(column.stream(), type);
         }

         private void setType(Stream<HiveCell> column, String type) {
             column.forEach(c -> c.reset(type));
         }

         List<String> headersAsString() {
             return (headersAsRow().createValues());
         }

         String AsString() {
             List<String> vS = H.list(ds().map(r -> "\n" + r.toString()));
             return H.join(vS);
         }

         String createTable() {
             String columnNamesTypes = H.join(H.zip(headersAsString(), " ", columnTypes()), ",");
             return "CREATE TABLE IF NOT EXISTS " + withSchema() + "\n" + H.paren(columnNamesTypes) + ";";
         }

         String dropTable() {
            return "DROP TABLE IF EXISTS " + withSchema() +  ";";
         }

         String truncateTable() {
            return "TRUNCATE TABLE " + withSchema() +  ";";
         }

         String inserts() {
             List<String> rowInserts = new ArrayList<>();
             for (HiveRow r : data()) rowInserts.add(r.insertValue());
             return "INSERT INTO TABLE " + withSchema() + " VALUES\n" + H.commaSeparateLines(rowInserts) + ";";
         }

         String select() {
             return  "SELECT "  + commaSeparatedColumnNames() + "\n" +
                     "FROM "    + withSchema() + "\n" +
                     "WHERE "   + "\n";
         }

         String delete() {
            return  "DELETE FROM " + withSchema() + "\n" +
                    "WHERE "   + "\n";
         }

         List<String> selects() { return H.nOf("\n" + select(), numRows()); }
         List<String> deletes() {
            return H.nOf("\n" + delete(), numRows());
        }

         List<String> keyColumns() {
             List<String> keys = H.keyColumns(tabConf, name);
             if (keys != null && keys.size() > 0)
                 return keys;
             else
                 return headers();
         }

         List<String> selectsWithWheres() {
             List<String> wheres = H.list((ds().map(r -> r.colNameEqualsValuesForWhere(keyColumns()))));
             if (wheres != null)
                 return H.zip(selects(), " ", wheres);
             else
                 return new ArrayList<>();
         }

         String allSelects() {
             return H.semicolonSeparateLines(selectsWithWheres());
         }

        List<String> deletesWithWheres() {
            List<String> wheres = H.list((ds().map(r -> r.colNameEqualsValuesForWhere(keyColumns()))));
            return H.zip(deletes(), " ", wheres);
        }

        String allDeletes() {
            return H.semicolonSeparateLines(deletesWithWheres());
        }

         public String toString() {
             return "\n"    + createTable() +
                     "\n\n" + inserts() +
                     "\n\n" + H.join(selectsWithWheres(), "\n");// +
             //      "\n "  + typedValuesAsString();
         }

         String fileName(String postFix)    {return name + postFix + ".hql"; }

         public boolean isInput()           {return Parser.isInputTable(excelName);}
         public boolean isExpected()        {return Parser.isExpectedTable(excelName);}

         public void saveHqlToFile() {
             //todo uncomment block below

             if (isInput()) {
                 saveCreatesToFile();
                 saveInsertsToFile();
             } else if (isExpected()){
                 saveSelectsToFile();
             }

             //todo delete 3 lines below
/*
             saveSelectsToFile();
             saveCreatesToFile();
             saveInsertsToFile();
*/
             saveDeletesToFile();
             saveDropsToFile();
             saveTruncatesToFile();
             //saveTabularToFile();
         }

         public void saveTabularToFile() {
            toFile(H.truncatesDirectory(confPath, modelName),fileName("_TAB"));
         }

         public void saveCreatesToFile()   { H.saveAsTextFile(H.createsDirectory(confPath, modelName),  fileName("_CRE"),  createTable());}
         public void saveInsertsToFile()   { H.saveAsTextFile(H.insertsDirectory(confPath, modelName),  fileName("_INS"),  inserts());}
         public void saveSelectsToFile()   { H.saveAsTextFile(H.selectsDirectory(confPath, modelName),  fileName("_SEL"),  allSelects());}
         public void saveDeletesToFile()   { H.saveAsTextFile(H.deletesDirectory(confPath, modelName),  fileName("_DEL"),  allDeletes());}
         public void saveDropsToFile()     { H.saveAsTextFile(H.dropsDirectory  (confPath, modelName),  fileName("_DRO"),  dropTable());}
         public void saveTruncatesToFile() { H.saveAsTextFile(H.truncatesDirectory(confPath, modelName),fileName("_TRU"),  truncateTable());}

    }

    public static void main(String[] args) throws Throwable{
        new CommandInterpreter(args).interpret();
        return;
    }
}
