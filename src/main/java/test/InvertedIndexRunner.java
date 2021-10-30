package test;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndexRunner {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(InvertedIndexRunner.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 检查参数所指定的输出路径是否存在，若存在，先删除
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        job.waitForCompletion(true);


        Job sortjob = new Job(conf, "Sort");
        sortjob.setJarByClass(InvertedIndexSort.class);
        sortjob.setMapperClass(InvertedIndexSort.Map.class);
        sortjob.setReducerClass(InvertedIndexSort.Reduce.class);

        // 设置Map输出类型
        sortjob.setMapOutputKeyClass(Text.class);
        sortjob.setMapOutputValueClass(Text.class);
        System.out.println("3");
        // 设置Reduce输出类型
        sortjob.setOutputKeyClass(Text.class);
        sortjob.setOutputValueClass(Text.class);

        //设置MultipleOutputs的输出格式
        //这里利用MultipleOutputs进行对文件输出
       // MultipleOutputs.addNamedOutput(sortjob, "topKMOS", TextOutputFormat.class, Text.class, Text.class);

        //System.out.println("3");
        // 设置输入和输出目录
        FileInputFormat.addInputPath(sortjob, output);
        //FileInputFormat.addInputPath(sortjob, new Path(output+"\\part-r-00000"));
        System.out.println("4");
        Path result = new Path("results");
        FileSystem fs1 = FileSystem.get(conf);
        if (fs1.exists(result)) {
            fs.delete(result, true);
        }
        FileOutputFormat.setOutputPath(sortjob, new Path("results"));
        sortjob.waitForCompletion(true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

