package test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class InvertedIndexSort {
    public static class Map extends Mapper<Object, Text, Text, Text> {

        // 输出key 词频
        Text outKey=new Text();
        //IntWritable outKey = new IntWritable();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("2");
            String line = value.toString();
            String[] arr = line.split("\t");
            String output = "";
            outKey.set(arr[0]);
            String[] mixfiles = arr[1].split(";");
            TreeMap<Integer, String> map = new TreeMap<Integer, String>(new Comparator<Integer>() {
                public int compare(Integer o1, Integer o2) {
                    if (o1 == o2) return 0;
                    return o1 < o2 ? 1 : -1;
                }
            });
            for (String i : mixfiles) {
                String[] file = i.split(":");
                map.put(new Integer(file[1]), file[0]);
            }

            Set<java.util.Map.Entry<Integer, String>> set = map.entrySet();
            for (java.util.Map.Entry<Integer, String> entry : set) {
                Integer thisvalue = entry.getKey();
                String thiskey = entry.getValue();
                output = output + thiskey + "#" + thisvalue.toString() + ",";
            }
            outValue.set(output);

            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                String element = st.nextToken();
               /* if (Pattern.matches("\\d+", element)) {
                    outKey.set(Integer.parseInt(element));
                } else {
                    System.out.println(element);
                    outValue.set(element);
                }*/
                 }

               context.write(outKey, outValue);
            }

    }


    /**
     * 根据词频排序
     *
     * @author zx
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {


//        private static MultipleOutputs<Text, Text> mos = null;


        //用TreeMap存储可以利用它的排序功能
        //这里用 WCString 因为TreeMap是对key排序，且不能唯一，而词频可能相同，要以词频为Key就必需对它封装
//        private static TreeMap<Integer, String> tm = new TreeMap<Integer, String>(new Comparator<Integer>() {
//            /**
//             * 默认是从小到大的顺序排的，现在修改为从大到小
//             * @param o1
//             * @param o2
//             * @return
//             */
//            public int compare(Integer o1, Integer o2) {
//                if (o1 == o2) return 0;
//                return o1 < o2 ? 1 : -1;
//            }

//        });

        /*
         * 以词频为Key是要用到reduce的排序功能
         */
/*
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                System.out.println(text + "+" + key);
                context.write(text, key);

            }
        }

        /*
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            String path = context.getConfiguration().get("final");
            mos = new MultipleOutputs<Text, Text>(context);
            Set<java.util.Map.Entry<Integer, String>> set = tm.entrySet();
            int count = 1;
            for (java.util.Map.Entry<Integer, String> entry : set) {
//                mos.write("topKMOS", new Text(entry.getValue()), new IntWritable(entry.getKey().getValue()), path);
                //mos.write("topKMOS", new Text(String.valueOf(count) + ": " + entry.getValue() + ", " + String.valueOf(entry.getKey().getKey())), NullWritable.get(), path);
                count += 1;
            }
            tm.clear();
            mos.close();
        }
        */

    }
}


