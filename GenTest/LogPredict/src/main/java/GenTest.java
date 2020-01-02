import com.sun.tools.javah.Gen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.jasper.tagplugins.jstl.core.Url;
import sun.misc.Timeable;

import javax.swing.plaf.multi.MultiToolTipUI;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
public class GenTest {
    public static int Start_Date = 8;
    public static int End_Date = 9;
    public static class LogTextOutputFormat extends TextOutputFormat<Text, IntWritable> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) {
            return new Path(getOutputName(context) + ".txt");
        }

    }

    public static class TimeTable implements Writable {

        private int Day_Length = End_Date - Start_Date + 1; //TODO: ..
        private int[][] Hour_Data = new int[24][Day_Length];

        public TimeTable() {
            for (int i = 0; i < 24; i++)
                for (int j = 0; j < Day_Length; j++)
                    Hour_Data[i][j] = 0;
        }

        public void Add_Info(int hour,int day,int fre){
            if((hour < 24 && hour > 0) && (day >7 && day < 22))
                Hour_Data[hour][day - 8] += fre;
        }

        public int Get_Info(int hour,int day){
            return Hour_Data[hour][day];
        }

        public void write(DataOutput out) throws IOException {
            for (int i = 0; i < 24; i++)
                for (int j = 0; j < Day_Length; j++)
                    out.writeInt(Hour_Data[i][j]);
        }

        public void readFields(DataInput in) throws IOException {
            for (int i = 0; i < 24; i++)
                for (int j = 0; j < Day_Length; j++)
                    Hour_Data[i][j] = in.readInt();
        }
    }

    public static class GenTestMapper extends Mapper<LongWritable,Text,Text,TimeTable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            String[] log = value.toString().split(" ");

            if(log.length != 10)
                return ;
            else {
                String Day_Key = log[1].substring(1, 3);
                String Month_Key = log[1].substring(4, 7);
                String Year_Key = log[1].substring(8, 12);
                String Second_Key = log[1].substring(13, 15);
                String Min_Key = log[1].substring(16, 18);
                String Hour_Key = log[1].substring(19, 21);

                String Url_Key = log[4].substring(0, log[4].length() - 1);
                Url_Key = Url_Key.replace("/","-"
                );
                if(Url_Key.substring(0,1).equals("-"))
                    Url_Key = Url_Key.substring(1,Url_Key.length());

                TimeTable t = new TimeTable();
                t.Add_Info(Integer.parseInt(Hour_Key), Integer.parseInt(Day_Key), 1);
                Text Url_Text = new Text(Url_Key);
                context.write(Url_Text, t);

            }

        }
    }

    public static class GenTestCombiner extends Reducer<Text,TimeTable,Text, TimeTable> {
        @Override
        public void reduce(Text key,Iterable<TimeTable> values,Context context) throws IOException,InterruptedException
        {
            TimeTable result = new TimeTable();
            for(TimeTable t : values)
            {
                int temp = 0;
                for (int i = 0; i < 24 ;i++)
                    for (int j = 0; j < End_Date - Start_Date + 1;j++) {
                        temp = t.Get_Info(i,j);
                        result.Add_Info(i,j + 8,temp);
                    }
            }
            context.write(key,result);
        }
    }

    public static class GenTestReducer extends Reducer<Text,TimeTable,Text,Text> {
        private MultipleOutputs<Text,Text> mos;
        private String output_path;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            Configuration conf = context.getConfiguration();
            output_path = conf.get("outputPath");
            mos = new MultipleOutputs<Text,Text>(context);
        }

        public void reduce(Text key,Iterable<TimeTable> values,Context context) throws IOException,InterruptedException
        {
            TimeTable result = new TimeTable();
            //String line = new String()
            for (TimeTable t : values)
            {
                for (int i = 0; i < 24; i++)
                for (int j = 0; j < End_Date - Start_Date + 1; j++)
                {
                    int temp = t.Get_Info(i,j);
                    result.Add_Info(i,j + 8,temp);
                }
            }

            for (int i = 0; i < 24; i++)
            {
                StringBuffer line = new StringBuffer();
                for (int j = 0 ; j < End_Date - Start_Date + 1; j++)
                {
                    line.append(result.Get_Info(i,j));
                    line.append('\t');
                }
                int next_hour = (i + 1) % 24;
                String hour_window = new String(String.format("%02d",i) + ":00 - " + String.format("%02d",next_hour) + ":00");

                mos.write("GenTest",new Text(hour_window),line,output_path + "/"+key);
            }

        }

        @Override
        public void cleanup(Context context) throws IOException,InterruptedException
        {
            mos.close();
        }
    }

    public static void main(String [] args)
    {
        try
        {
            Path path = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(new URI(args[1]),new Configuration());
            if(fileSystem.exists(path))
                fileSystem.delete(path,true);

            Configuration conf = new Configuration();
            conf.set("outputPath",args[1]);
            Job job = Job.getInstance(conf,"GenTest");

            job.setJarByClass(GenTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TimeTable.class);


            job.setMapperClass(GenTestMapper.class);
            job.setCombinerClass(GenTestCombiner.class);
            job.setReducerClass(GenTestReducer.class);

            fileSystem = FileSystem.get(conf);

            FileStatus [] fileStatusArr = fileSystem.globStatus(new Path(args[0] + "/*.log"), new PathFilter() {
                public boolean accept(Path path) {
                    String regex = "(?!.*2015-09-22).*$";
                    return path.toString().matches(regex);
                //  return false;
                }
            });

            for(FileStatus fileStatus : fileStatusArr)
                {
                    FileInputFormat.addInputPath(job,fileStatus.getPath());
                }
            MultipleOutputs.addNamedOutput(job,"GenTest",LogTextOutputFormat.class,Text.class,Text.class);
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            LazyOutputFormat.setOutputFormatClass(job, LogTextOutputFormat.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
