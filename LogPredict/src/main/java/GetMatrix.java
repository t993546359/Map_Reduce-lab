import java.io.*;

public class GetMatrix {//本地执行，调用GradientDescent求线性回归的参数矩阵并打印出来

    private static String filepath="/home/tto/IdeaProjects/LogPredict/src/main/resources/alldays";
    private static int days = 14;
    private static double a = 0.85;//使用指数平滑中的系数

    public static boolean ComputeMatrix() {
        try {
            File file = new File(filepath);
            if (file.isDirectory()) {
                System.out.println("文件路径正确");
                String[] filelist = file.list();
                int filenum = filelist.length;//文件数，也就是接口数
                BufferedReader[] filereader=new BufferedReader[filenum];

                for (int i = 0; i < filenum; i++) {
                    File eachfile = new File(filepath + "/" + filelist[i]);
                    filereader[i] = new BufferedReader(new FileReader(eachfile));
                }
                for (int hour=0;hour<24;hour++)
                {
                    //对每个时间段,创建接口数*（天数+1）的矩阵，用于1*(days+1)系数矩阵
                    //24行综合在一起可得到24*（days+1）的系数矩阵
                    double[][] testdata=new double[filenum][days+1];
                    //依次读取每个文件（接口）的数据
                    for (int i=0;i<filenum;i++)
                    {
                        String line=filereader[i].readLine();//下次循环就读到下一行（下一时间段）

                        //下面模仿指数平滑操作得到最后一列数据添加到数据集中用于生成系数矩阵
                        String[] day=line.split("\t");
                        double res = Integer.parseInt(day[1]);
                        for(int j = 2; j <= days; j++)
                        {
                            double nextday = Integer.parseInt(day[j]);
                            res= res*(1-a) + nextday*a;
                        }
                        for (int j=0;j<days;j++)
                            testdata[i][j]=Integer.parseInt(day[j+1]);
                        testdata[i][days]=res;
                    }
                    //对testdays数据集调用梯度下降算法得到系数矩阵
                    GradientDescent m = new GradientDescent(testdata,filenum,days+1,0.001,50000);
                    //   m.printTrainData();
                    m.trainTheta();
                    m.printTheta();
                    System.out.println();
                }

            } else if (!file.isDirectory()) {

                System.out.println("文件路径错误");
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件读取异常:" + e.getMessage());
        }
        return true;
    }

    public static void main(String[] args) {
        ComputeMatrix();
    }
}
