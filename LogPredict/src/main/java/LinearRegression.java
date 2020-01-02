import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class LinearRegression{


    public static class LinearRegressionMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private MultipleOutputs<Text,Text> prediction;
        private String out_path;
        private static int days = 14;//days在本地测试时根据使用多少天数据而改变
        //下面为系数矩阵，通过运行getmatrix函数得出，列数为训练集的天数+1
        public double[][] matrix={
                {0.10 ,0.04 ,-0.00 ,0.03 ,-0.03 ,0.05 ,0.03 ,-0.03 ,0.09 ,0.10 ,0.10 ,0.10 ,0.10 ,0.10 ,0.10},
                {0.10 ,-0.07 ,0.01 ,0.05 ,0.04 ,0.05 ,0.07 ,0.00 ,0.11 ,-0.03 ,0.11 ,0.09 ,0.11 ,0.16 ,0.26},
                {0.10 ,-0.07 ,0.04 ,-0.01 ,0.03 ,0.01 ,0.05 ,0.05 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12},
                {0.10 ,-0.00 ,0.04 ,-0.06 ,0.04 ,-0.00 ,0.04 ,0.03 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13 ,0.13},
                {0.10 ,-0.04 ,0.02 ,0.02 ,0.02 ,-0.03 ,0.01 ,0.02 ,0.13 ,0.14 ,0.13 ,0.14 ,0.13 ,0.14 ,0.14},
                {0.10 ,0.09 ,-0.01 ,0.01 ,-0.06 ,-0.03 ,-0.03 ,-0.02 ,0.14 ,0.09 ,0.09 ,0.10 ,0.10 ,0.20 ,0.21},
                {0.10 ,-0.05 ,0.04 ,0.03 ,0.01 ,-0.02 ,0.03 ,0.05 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11},
                {0.10 ,-0.07 ,0.03 ,0.04 ,0.03 ,0.00 ,0.04 ,0.03 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11 ,0.11},
                {0.10 ,-0.05 ,0.01 ,0.04 ,0.03 ,-0.03 ,0.04 ,0.04 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12},
                {0.10 ,-0.01 ,-0.03 ,0.07 ,0.00 ,0.00 ,-0.02 ,0.01 ,0.14 ,0.14 ,0.13 ,0.13 ,0.06 ,0.11 ,0.19},
                {0.10 ,-0.02 ,0.03 ,0.10 ,-0.01 ,-0.05 ,-0.05 ,0.04 ,0.16 ,0.14 ,0.13 ,0.14 ,0.02 ,0.05 ,0.22},
                {0.10 ,0.02 ,-0.02 ,0.09 ,-0.02 ,-0.06 ,-0.01 ,0.04 ,0.15 ,0.14 ,0.14 ,0.17 ,0.03 ,0.07 ,0.21},
                {0.10 ,0.01 ,-0.09 ,0.08 ,0.05 ,0.02 ,0.06 ,0.05 ,0.12 ,0.12 ,0.12 ,0.12 ,0.11 ,0.12 ,0.13},
                {0.10 ,0.02 ,-0.08 ,0.06 ,0.05 ,0.02 ,0.03 ,0.02 ,0.15 ,0.14 ,0.14 ,0.17 ,0.04 ,0.09 ,0.21},
                {0.10 ,0.05 ,-0.05 ,0.15 ,0.07 ,-0.07 ,-0.13 ,0.01 ,0.09 ,0.13 ,0.20 ,0.17 ,0.01 ,0.04 ,0.35},
                {0.10 ,0.02 ,-0.07 ,0.12 ,0.09 ,-0.03 ,-0.07 ,0.01 ,0.18 ,0.15 ,0.13 ,0.14 ,0.05 ,0.03 ,0.23},
                {0.10 ,0.01 ,-0.06 ,0.03 ,0.08 ,-0.04 ,-0.03 ,0.09 ,0.15 ,0.14 ,0.16 ,0.13 ,-0.02 ,0.06 ,0.29},
                {0.10 ,-0.07 ,-0.04 ,0.09 ,0.09 ,-0.01 ,-0.02 ,0.12 ,0.13 ,0.12 ,0.14 ,0.12 ,0.00 ,0.08 ,0.25},
                {0.10 ,-0.01 ,-0.07 ,0.03 ,0.04 ,-0.05 ,0.05 ,0.15 ,0.04 ,0.12 ,0.03 ,0.12 ,0.12 ,0.12 ,0.30},
                {0.10 ,-0.01 ,-0.05 ,0.02 ,0.02 ,-0.03 ,0.02 ,0.10 ,-0.04 ,0.13 ,0.13 ,0.12 ,0.13 ,0.13 ,0.33},
                {0.10 ,-0.02 ,0.07 ,0.10 ,-0.01 ,-0.13 ,-0.02 ,0.08 ,0.09 ,0.07 ,0.13 ,-0.07 ,0.13 ,0.19 ,0.39},
                {0.10 ,0.05 ,0.02 ,0.02 ,0.03 ,-0.07 ,-0.00 ,0.22 ,0.12 ,-0.08 ,0.12 ,0.03 ,0.11 ,0.12 ,0.34},
                {0.10 ,0.02 ,-0.06 ,0.00 ,0.03 ,-0.01 ,0.02 ,0.11 ,0.11 ,-0.11 ,0.11 ,0.11 ,0.11 ,0.21 ,0.36},
                {0.10 ,0.01 ,-0.04 ,0.01 ,0.02 ,0.00 ,0.03 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.12 ,0.13},
        };

        @Override
        public void setup(Context context)//初始化
        {
            Configuration conf = context.getConfiguration();
            out_path = conf.get("out_path");
            prediction = new MultipleOutputs<Text, Text>(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String file_name = fileSplit.getPath().getName();//得到文件名
            String interface_name = file_name.replace(".txt", "");//去掉后缀得到接口名

            String[] day = value.toString().split("\t");
            // System.out.println(value.toString());

            if(day.length == days+1)//1列时间+数据
            {
                String hour = day[0].split(":")[0];//day[0]为时间段
                int row = Integer.parseInt(hour);//决定使用第几行的权重,即第几个时间段
                double res =matrix[row][0] * 1;//在计算系数矩阵时开头加了恒为1的一列，对应第一列系数
                for(int i = 1; i <=days; i++)
                {
                    res+=matrix[row][i]*Integer.parseInt(day[i]);
                }
                prediction.write("LinearRegression",new Text(day[0]), new Text( String.format("%.2f", res)), out_path+"/"+interface_name );

            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            prediction.close();
        }
    }

    //设置输出格式
    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text>
    {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            Path out_path;
            out_path = new Path(getOutputName(context)+".txt");
            return out_path;
        }
    }

    public static void main(String[] args)
    {
        try{
            // 若输出目录存在,则删除
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new URI(args[1]), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);


            Configuration conf = new Configuration();
            conf.set("out_path", args[1]);
            Job job = Job.getInstance(conf,"LinearRegression");

            job.setJarByClass(LinearRegression.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(LinearRegression.LinearRegressionMapper.class);

            fileSystem = FileSystem.get(conf);

            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "LinearRegression", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
            //return 0;

        }catch (Exception e)
        {
            e.printStackTrace();
            //return -1;
        }
    }

}
