import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.net.URI;

public class ComputeRMSE {
    public static class ComputeRMSEMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();    //得到文件名
            //System.out.println(filename);
            if(filename.contains(".txt")&&(!filename.contains(".crc")))
            {
                String[] log = value.toString().split("\t");
                //System.out.println(value.toString());
                String time = log[0];
                String file_name = ((FileSplit) context.getInputSplit()).getPath() .getName();
                String interface_name = file_name.replace(".txt","");

                String num = log[1];
                context.write(new Text(time+"_"+interface_name ),new Text(num));
            }

        }

    }
    public static class ComputeRMSEPartitioner extends HashPartitioner<Text, Text>
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            String[] keyInfo = key.toString().split("_");
            return super.getPartition(new Text(keyInfo[0]), value, numReduceTasks);
        }
    }
    public static class ComputeRMSEReducer extends Reducer<Text, Text, Text, Text>
    {
        private String out_path;
        private MultipleOutputs<Text,Text> prediction;

        private double RMSE=0;
        private double sum=0;
        private double diff_square=0;//差值平方
        private double interface_num=0;//接口数
        private String hour=new String();


        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            out_path = conf.get("out_path");
            prediction = new MultipleOutputs<Text, Text>(context);
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String[] keyInfo=key.toString().split("_");
            String new_hour=keyInfo[0];
            int flag=0;
            double C1 = 0,C2 = 0,diff = 0;
            for(Text val: values)
            {
                if (flag==0)
                    C1 = Double.parseDouble(val.toString());
                    //diff=Double.parseDouble(val.toString());
                else if (flag==1) {
                    C2 = Double.parseDouble(val.toString());
                    diff = C2 - C1;
                }
                else
                    break;
                flag++;
            }
            if (hour.length()!=0&&hour.compareTo(new_hour)!=0)//不是一个时间窗
            {
                sum += Math.sqrt(diff_square/interface_num);
                interface_num=1;
                diff_square=diff*diff;
            }
            else//同一个时间窗
            {
                interface_num++;
                diff_square+=diff*diff;
            }
            hour=new_hour;//更新时间段
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            sum+=Math.sqrt(diff_square/interface_num);
            RMSE = sum/24;
            //context.write(new Text("RMSE:"),new Text(Double.toString(RMSE)));
            prediction.write(new Text("RMSE = "),new Text(Double.toString(RMSE)),out_path+"/RMSE");
            prediction.close();
        }
    }

    public static void main(String[] args)
    {
        // 若输出目录存在,则删除
        try {
            Path path = new Path(args[2]);
            FileSystem fileSystem = FileSystem.get(new URI(args[2]), new Configuration());
            if (fileSystem.exists(path))
                fileSystem.delete(path, true);

            Configuration conf = new Configuration();
            conf.set("out_path", args[2]);
            Job job = Job.getInstance(conf,"ComputeRMSE");

            job.setJarByClass(ComputeRMSE.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(ComputeRMSEMapper.class);
            job.setReducerClass(ComputeRMSEReducer.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);


        }catch (Exception e)
        {
            e.printStackTrace();
            //return -1;
        }
        /*try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "ComputeRMSE");
            job.setJarByClass(ComputeRMSE.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(ComputeRMSEMapper.class);
            job.setReducerClass(ComputeRMSEReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileInputFormat.addInputPath(job, new Path("/user/2019st35/testset"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/RMSE"));
            job.waitForCompletion(true);
            //return 0;
        } catch (Exception e) {
            e.printStackTrace();
            //return -1;
        }*/
    }
}
