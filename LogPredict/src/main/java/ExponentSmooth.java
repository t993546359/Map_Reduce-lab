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

public class ExponentSmooth {

    //指数平滑方法进行预测
    public static class ExponentSmoothMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private String out_path;
        private static double a = 0.85;//参数
        private MultipleOutputs<Text,Text> prediction;//预测结果
        private static int days = 14;//days在本地测试时根据使用多少天数据而改变

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
            FileSplit filesplit = (FileSplit)context.getInputSplit();
            String file_name = filesplit.getPath().getName();//得到文件名
            String interface_name = file_name.replace(".txt", "");//去掉后缀得到接口名

            String[] day = value.toString().split("\t");
            if(day.length == days+1)//1列时间+数据
            {
                double res = Integer.parseInt(day[1]);
                for(int i = 2; i <= days; i++)
                {
                    double nextday = Integer.parseInt(day[i]);
                    res= res*(1-a) + nextday*a;
                }
                prediction.write("ExponentSmooth",new Text(day[0]), new Text( String.format("%.2f", res)), out_path+"/"+interface_name );
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

    public static void main (String[] args)
    {

        try{

            // 若输出目录存在,则删除
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new URI(args[1]), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);

            Configuration conf = new Configuration();
            conf.set("out_path", args[1]);
            Job job = Job.getInstance(conf,"ExponentSmooth");


            job.setJarByClass(ExponentSmooth.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(ExponentSmooth.ExponentSmoothMapper.class);

            fileSystem = FileSystem.get(conf);

            FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(args[0]+"/*.txt"));
            for(FileStatus fileStatus : fileStatusArray){
                path = fileStatus.getPath();
                FileInputFormat.addInputPath(job, path);
            }
            MultipleOutputs.addNamedOutput(job, "ExponentSmooth", LogTextOutputFormat.class, Text.class, Text.class);
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
