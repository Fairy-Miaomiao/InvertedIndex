package test;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Text keyInfo = new Text();// 存储单词和 URL 组合
    private static final Text valueInfo = new Text("1");// 存储词频,初始化为1
    private Text word = new Text();
    private Set<String> stopWordList = new HashSet<String>();
    private Set<String> puncList = new HashSet<String>();

    protected void setup(Context context){
        // 停词文件路径
        Path stopWordFile = new Path("F:\\FBDP\\作业\\作业5\\stop-word-list.txt");
        // 标点符号文件路径
        Path puncFile = new Path("F:\\FBDP\\作业\\作业5\\punctuation.txt");
        readWordFile(stopWordFile);
        readPuncFile(puncFile);
        //System.out.println(stopWordList);
    }

    private void readWordFile(Path stopWordFile) {
        try {
            BufferedReader fis1 = new BufferedReader(new FileReader(stopWordFile.toString()));
            String stopWord = null;
            while ((stopWord = fis1.readLine()) != null) {
                stopWordList.add(stopWord);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file '"
                    + stopWordFile + "' : " + ioe.toString());
        }
    }
    private void readPuncFile(Path puncFile) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(puncFile.toString()));
            String punc;
            //System.out.println(stopWordFile.toString());
            while ((punc = fis.readLine()) != null) {
                puncList.add(punc);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading punctuation file '"
                    + puncFile + "' : " + ioe.toString());
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = StringUtils.split(line, " ");// 得到字段数组
        StringTokenizer tokenizer = new StringTokenizer(line);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();// 得到这行数据所在的文件切片
        String fileName = fileSplit.getPath().getName();// 根据文件切片得到文件名
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            token = token.toLowerCase();
            Pattern pp=Pattern.compile("[\\d]");
            Matcher matcher=pp.matcher(token);
            token=matcher.replaceAll("");

            //System.out.println(token);
            if (token.length() >= 3 && !stopWordList.contains(token)) {
                for(String puncWord: puncList){
                    token = token.replace(puncWord.substring(1), "");
                }
                if(token.length() >= 3 && !stopWordList.contains(token)){
                    keyInfo.set(token + ":" + fileName);
                    context.write(keyInfo, valueInfo);
                }
            }
        }
        /*
        FileSplit fileSplit = (FileSplit) context.getInputSplit();// 得到这行数据所在的文件切片
        String fileName = fileSplit.getPath().getName();// 根据文件切片得到文件名
        System.out.println(fileName);

        for (String field : fields) {
            // key值由单词和URL组成，如“MapReduce:file1”
            keyInfo.set(field + ":" + fileName);
            context.write(keyInfo, valueInfo);
        }*/


    }
}
