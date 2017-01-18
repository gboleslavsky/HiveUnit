package com.gboleslavsky.hut;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.gboleslavsky.hut.Hql.*;

public class CommandInterpreter {

    private interface Executable { void execute();}

    JCommander commander;

    private CommandInterpreter(){};

    public CommandInterpreter(String[] args) throws IOException{
        Arrays.stream(args).forEach(a->System.out.println(a));
        commander = new JCommander(new CommandInterpreter());
        ExcelToHql eth = new ExcelToHql();
        commander.addCommand("eth", eth);
        Load l = new Load();
        commander.addCommand("load", l);
        RunBusinessLogic b = new RunBusinessLogic();
        commander.addCommand("hive", b);
        CompareActualToExpected c = new CompareActualToExpected();
        commander.addCommand("assert", c);
        Clean cl = new Clean();
        commander.addCommand("clean", cl);
        commander.parse(args);
    }

    private Executable command(){
        return (Executable) commander.getCommands().get(commander.getParsedCommand()).getObjects().get(0);
    }


    public void interpret() { command().execute(); }

    //Commands ======================================================================================================
    @Parameters(separators = "=", commandDescription = "Parse input data and save as .hql files")
    private class ExcelToHql implements Executable{
        @Parameter(names="-cp", required= true, description = "Absolute path to folder with config file.")
        private String confPath;
        public void execute() {
            try {
                H.excelToHql(confPath);
            } catch (Exception e) {
                System.out.println("File system problems setting up hql data");
                e.printStackTrace();
            }
        }
    }

    @Parameters(separators = "=", commandDescription = "Run creates and then inserts")
    private class Load implements Executable{
        @Parameter(names="-cp", required= true, description = "Absolute path to folder with config file.")
        private String confPath;
        public void execute() {
            try {
                H.loadHqlData(confPath);
            } catch (Exception e) {
                System.out.println("File system problems setting up hql data");
                e.printStackTrace();
            }
        }
    }
    //


    @Parameters(separators = "=", commandDescription = "Execute .hql files on Spark-based hive interpreter")
    private class RunBusinessLogic  implements Executable{
        @Parameter(names="-cp", required= true, description = "Absolute path to folder with config file.")
        private String confPath;

        public void execute(){
            H.scriptsToRun(confPath).stream().forEach(s-> LocalSparkHqlRunner.runScript(H.fileToString(s)));
        }
    }

    @Parameters(separators = "=", commandDescription = "Execute .hql files on Spark-based hive interpreter")
    private class Clean implements Executable{
        @Parameter(names="-cp", required= true, description = "Absolute path to folder with config file.")
        private String confPath;

        public void execute(){
            try {
                H.removeHqlData(confPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Parameters(separators = "=", commandDescription =  "converts expected .xlsx sheets to expected tabular instance" +
                                                        "then runs SELECTs to generate actual tabular instance\n" +
                                                        "and diffs the 2 tabulars to produce a results tabular\n")

    private class CompareActualToExpected  implements Executable{
        @Parameter(names="-cp", required= true, description = "Absolute path to folder with config file.")
        private String confPath;

        public void execute() {
            List<HiveTable> all = new ArrayList<>();
            for (Path p : H.excelFiles(confPath)) {
                all.addAll(Parser.hiveTables(p, confPath));
            }
            List<HiveTable> expecteds = H.list(all.stream().filter(t -> t.isExpected()));
            for (HiveTable expected : expecteds) {
                Tabular actual = H.actual(expected);
                String resultsDir = H.resultsDirectory(expected.modelName);
                String name = expected.name;
                expected.toFile     (resultsDir, name + "_exp"  + ".txt");
                actual.toFile       (resultsDir, name + "_act"  + ".txt");
                expected.diffsToFile(resultsDir, name+ "_diffs" + ".txt", expected, actual);
            }
        }
    }
}
