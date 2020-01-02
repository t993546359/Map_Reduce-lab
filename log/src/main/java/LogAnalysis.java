import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

public class LogAnalysis {


    public static class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println("start map");
            String[] log = value.toString().split(" ");
            if (log.length == 10) {

                /*log_ip*/
                String log_ip = log[0];
                /*log_time   (delete day/month.year)*/
                String log_time = log[1];
                log_time = log_time.substring(13);
                /*log_time_zone  delete ]  all time zone are +8000   */
                String log_time_zone = log[2];
                log_time_zone = log_time_zone.substring(0, 4);
                /*log_http_method_1   delete " */
                String log_http_method_1 = log[3];
                log_http_method_1 = log_http_method_1.substring(1);
                /*log_url   delete the first "/"  and  we need replace all /   to  -  */
                String log_url = log[4];
                log_url = log_url.substring(1);
                log_url = log_url.replaceAll("/", "-");
                /*log_http   */
                String log_http = log[5];
//                log_http = log_http.substring(0, 7);
                /*log_http_method_2    get */
                String log_http_method_2 = log[6];
                /*log_status_code    200 404 500 */
                String log_status_code = log[7];
                /*log_response_longth    byte longth*/
                String log_response_longth = log[8];
                /*log_response_time   */
                String log_response_time = log[9];
                /*example 00:19:16  to 00:00-01:00*/
                String format_time = Get_Format_Time(log_time);
                Text value_1 = new Text("1");
                Text key_all_log_status_code = new Text("1" + "_" + "00" + ":" + "00" + "-" + "00" + ":" + "00" + "_" + log_status_code);
                Text key_log_status_code = new Text("1" + "_" + format_time + "_" + log_status_code);
                Text key_ip = new Text("2" + "_" + log_ip);
                Text key_ip_time = new Text("2_" + log_ip + "_" + format_time);
                Text key_url = new Text("3_" + log_url);
                Text key_url_time = new Text("3_" + log_url + "_" + log_time);
                Text key_4_url = new Text("4_" + log_url);
                Text key_4_url_time = new Text("4_" + log_url + "_" + format_time);
                Text value_response_time = new Text(log_response_time + "_" + "1");
//                task1
                context.write(key_all_log_status_code, value_1);
                context.write(key_log_status_code, value_1);
//                task2
                context.write(key_ip, value_1);
                context.write(key_ip_time, value_1);
//                task3
                context.write(key_url, value_1);
                context.write(key_url_time, value_1);
//                task4
                context.write(key_4_url, value_response_time);
                context.write(key_4_url_time, value_response_time);
            }
        }

        private String Get_Format_Time(String time) {
            time = time.split(":")[0];
            int first_time = Integer.parseInt(time);
            int second_time = first_time + 1;
            /*if secod_time is 24:**   we need change it to 00:** */
            if (second_time == 24) {
                second_time = 0;
            }
            /*return formatting time */
            return (String.format("%02d", first_time) + ":" + "00" + "-" + String.format("%02d", second_time) + ":" + "00");
        }
    }

    public static class LogAnalysisCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            System.out.println("start combiner");
            String[] key1 = key.toString().split("_");

            int task = Integer.parseInt(key1[0]);
            if (task == 1 || task == 2 || task == 3) {
                int sum = 0;
                for (Text t : values) {
                    int index;
                    index = Integer.parseInt(t.toString());
                    sum = sum + index;
                }
                Text c_sum;
                c_sum = new Text("" + sum);
                context.write(key, c_sum);
            } else {
                int index = 0;
                double time = 0;
                for (Text t : values) {
                    time += (Double.parseDouble(t.toString().split("_")[0]) * (Integer.parseInt(t.toString().split("_")[1])));
                    index = index + Integer.parseInt(t.toString().split("_")[1]);
                }
                Text c_sum;
                c_sum = new Text("" + time / index + "_" + index);
                context.write(key, c_sum);
            }

        }
    }

    public static class LogAnalysisPartitioner extends HashPartitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int sum) {
            System.out.println("Start partitioner");

            if (key.toString().split("_").length == 2)
                return super.getPartition(key, value, sum);
            else
                return super.getPartition(new Text(key.toString().split("_")[0] + "_" + key.toString().split("_")[1]), value, sum);
        }
    }

    public static class LogAnalysisReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> Mul_Out_put;
        private String[] OutputPath;
        private Integer flag1 = 0;
        private Integer flag2 = 0;
        private Integer flag3 = 0;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            OutputPath = new String[4];
            OutputPath[0] = conf.get("outputPath1");
            OutputPath[1] = conf.get("outputPath2");
            OutputPath[2] = conf.get("outputPath3");
            OutputPath[3] = conf.get("outputPath4");
            Mul_Out_put = new MultipleOutputs<Text, Text>(context);
        }
        private static String flag = new String();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("start reduce");
            int task = Integer.parseInt(key.toString().split("_")[0]);
            Text Status_Code_200_flag=new Text("200" + ":");
            Text Status_Code_404_flag=new Text("404" + ":");
            Text Status_Code_500_flag=new Text("500" + ":");
            Text Status_Code_200_Count=new Text(flag1.toString());
            Text Status_Code_404_Count=new Text(flag2.toString());
            Text Status_Code_500_Count=new Text(flag3.toString());
            Text Status_Code =new Text(" 200:" + flag1.toString() + " 404:" + flag2.toString() + " 500:" + flag3.toString());


            if (task == 1) {
                int sum_1 = 0;
                for (Text v : values)
                    sum_1 =sum_1+ Integer.parseInt(v.toString());
                if (flag.compareTo(key.toString().split("_")[1]) != 0 && flag.length() != 0)
                {
                    if (flag.compareTo("00:00-00:00") == 0) {

                        Mul_Out_put.write("Task1", Status_Code_200_flag, Status_Code_200_Count, OutputPath[0] + "/1");
                        Mul_Out_put.write("Task1", Status_Code_404_flag, Status_Code_404_Count, OutputPath[0] + "/1");
                        Mul_Out_put.write("Task1", Status_Code_500_flag, Status_Code_500_Count, OutputPath[0] + "/1");

                    } else {
                        Mul_Out_put.write("Task1", new Text(flag), Status_Code, OutputPath[0] + "/1");
                    }
                    flag1 = 0;
                    flag2 = 0;
                    flag3 = 0;
                }
                if (key.toString().split("_")[2].compareTo("200") == 0)
                    flag1 = sum_1;
                else if (key.toString().split("_")[2].compareTo("404") == 0)
                    flag2 = sum_1;
                else if (key.toString().split("_")[2].compareTo("500") == 0)
                    flag3 = sum_1;
                flag = key.toString().split("_")[1];
            }
            if (task == 2) {
                int sum2 = 0;
                for (Text t : values)
                    sum2 += Integer.parseInt(t.toString());
                if (key.toString().split("_").length == 2) {

                    Mul_Out_put.write("Task2",new Text(key.toString().split("_")[1] + ":"), new Text("" + sum2), OutputPath[1] + "/" + key.toString().split("_")[1]);
                } else {

                    Mul_Out_put.write("Task2", new Text(key.toString().split("_")[2]), new Text("" + sum2), OutputPath[1] + "/" + key.toString().split("_")[1]);
                }
            }
            if (task == 3) {
                int sum = 0;
                for (Text t : values)
                    sum += Integer.parseInt(t.toString());
                if (key.toString().split("_").length == 2) {
                    Mul_Out_put.write("Task3", new Text(key.toString().split("_")[1] + ":"), new Text("" + sum), OutputPath[2] + "/" + key.toString().split("_")[1]);
                } else {
                    Mul_Out_put.write("Task3", new Text(key.toString().split("_")[2]), new Text("" + sum), OutputPath[2] + "/" + key.toString().split("_")[1]);
                }
            }
            if (task == 4) {
                int n = 0;
                double time = 0;
                for (Text t : values) {
                    String[] valueInfo = t.toString().split("_");
                    int ni = Integer.parseInt(valueInfo[1]);
                    time += (Double.parseDouble(valueInfo[0]) * ni);
                    n += ni;
                }
                if (key.toString().split("_").length == 2) {
                    Mul_Out_put.write("Task4", new Text(key.toString().split("_")[1] + ":"), new Text(String.format("%.2f", time / n)), OutputPath[3] + "/" + key.toString().split("_")[1]);
                } else {
                    Mul_Out_put.write("Task4", new Text(key.toString().split("_")[2]), new Text(String.format("%.2f", time / n)), OutputPath[3] + "/" + key.toString().split("_")[1]);
                }
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text Status_Code_200_flag=new Text("200" + ":");
            Text Status_Code_404_flag=new Text("404" + ":");
            Text Status_Code_500_flag=new Text("500" + ":");
            Text Status_Code_200_Count=new Text(flag1.toString());
            Text Status_Code_404_Count=new Text(flag2.toString());
            Text Status_Code_500_Count=new Text(flag3.toString());
            Text Status_Code =new Text(" 200:" + flag1.toString() + " 404:" + flag2.toString() + " 500:" + flag3.toString());
            if (flag.compareTo("00:00-00:00") == 0) {
                Mul_Out_put.write("Task1", Status_Code_200_flag, Status_Code_200_Count, OutputPath[0] + "/1");
                Mul_Out_put.write("Task1", Status_Code_404_flag, Status_Code_404_Count, OutputPath[0] + "/1");
                Mul_Out_put.write("Task1", Status_Code_500_flag, Status_Code_500_Count, OutputPath[0] + "/1");

            } else {
                Mul_Out_put.write("Task1", new Text(flag), Status_Code, OutputPath[0] + "/1");
            }
            Mul_Out_put.close();
        }
    }

    public static class LogTextOutputFormat extends TextOutputFormat<Text, Text> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            Path out_path;
            out_path = new Path(getOutputName(context) + ".txt");
            return out_path;
        }

    }

    public static void main(String[] args) {
        try {


            Configuration conf = new Configuration();

            conf.set("outputPath1", args[1]);
            conf.set("outputPath2", args[2]);
            conf.set("outputPath3", args[3]);
            conf.set("outputPath4", args[4]);
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", " ");

            Job job = Job.getInstance(conf, "LogAnalysis");

            job.setJarByClass(LogAnalysis.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setMapperClass(LogAnalysisMapper.class);
            job.setCombinerClass(LogAnalysisCombiner.class);
            job.setPartitionerClass(LogAnalysisPartitioner.class);
            job.setReducerClass(LogAnalysisReducer.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));

            MultipleOutputs.addNamedOutput(job, "Task1", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task2", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task3", LogTextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "Task4", LogTextOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}