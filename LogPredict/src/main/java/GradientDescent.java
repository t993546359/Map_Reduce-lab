import java.io.*;


public class GradientDescent {//梯度下降法,被getmatrix调用来求线性回归的参数矩阵
     /*
     * 训练数据示例：
     *   x0        x1        x2        y
        1.0       1.0       2.0       7.2
        1.0       2.0       1.0       4.9
        1.0       3.0       0.0       2.6
        1.0       4.0       1.0       6.3
        1.0       5.0      -1.0       1.0
        1.0       6.0       0.0       4.7
        1.0       7.0      -2.0      -0.6
        注意！！！！x1，x2，y三列是用户实际输入的数据，x0是为了推导出来的公式统一，特地补上的一列,恒为1
        x0,x1,x2是“特征”，y是结果

        h(x) = theta0 * x0 + theta1* x1 + theta2 * x2

        theta0,theta1,theta2 是想要训练出来的参数

     *
     */

    private double [][] trainData;//训练数据，一行一个数据，每一行最后一个数据为 y
    private int row;//训练数据  行数
    private int col;//训练数据 列数

    private double [] theta;//参数theta

    private double alpha;//训练步长
    private int iteration;//迭代次数


    public GradientDescent(double[][] traindata,int Row,int Col,double alpha,int iteration)
    {

        trainData = new double[Row][Col+1];//这里需要注意，为什么要+1，因为为了使得公式整齐，我们加了一个特征x0，x0恒等于1
        for (int i=0;i<Row;i++)
        {
            trainData[i][0]=1.0/10000.0;
            for (int j = 1; j <= Col; j++) {
                trainData[i][j] = traindata[i][j-1] / 10000.0;
            }

        }
        this.row=Row;
        this.col=Col+1;

        this.alpha = alpha;
        this.iteration=iteration;

        theta = new double [col-1];//h(x)=theta0 * x0 + theta1* x1 + theta2 * x2 + .......
        initialize_theta();

    }


    private void initialize_theta()//将theta各个参数全部初始化为1.0
    {
        for(int i=0;i<theta.length;i++)
            theta[i]=0.1;
    }

    public void trainTheta()
    {
        int iteration = this.iteration;
        //double dis = Double.MAX_VALUE;
        while( (iteration--)>0 )
        {
            //对每个theta i 求 偏导数
            double [] partial_derivative = compute_partial_derivative();//偏导数
            //更新每个theta
            for(int i =0; i< theta.length;i++)
                theta[i]-= alpha * partial_derivative[i];
        }
    }

    private double [] compute_partial_derivative()
    {
        double [] partial_derivative = new double[theta.length];
        for(int j =0;j<theta.length;j++)//遍历，对每个theta求偏导数
        {
            partial_derivative[j]= compute_partial_derivative_for_theta(j);//对 theta i 求 偏导
        }
        return partial_derivative;
    }

    private double compute_partial_derivative_for_theta(int j)
    {
        double sum=0.0;
        for(int i=0;i<row;i++)//遍历 每一行数据
        {
            sum+=h_theta_x_i_minus_y_i_times_x_j_i(i,j);
        }
        String s = String.format("%.2f", sum/row);
        //System.out.println(s);
        Double temp= Double.valueOf(s);
        return temp;
    }

    private double h_theta_x_i_minus_y_i_times_x_j_i(int i,int j)
    {
        double[] oneRow = getRow(i);//取一行数据，前面是feature，最后一个是y
        double result = 0.0;

        for(int k=0;k< (oneRow.length-1);k++)
            result+=theta[k]*oneRow[k];
        result-=oneRow[oneRow.length-1];
        result*=oneRow[j];
        return result;
    }
    private double [] getRow(int i)//从训练数据中取出第i行，i=0，1，2，。。。，（row-1）
    {
        return trainData[i];
    }


    public void printTrainData()
    {
        System.out.println("Train Data:\n");
        for(int i=0;i<col-1;i++)
            System.out.printf("%10s","x"+i+" ");
        System.out.printf("%10s","y"+" \n");
        for(int i=0;i<row;i++)
        {
            for(int j=0;j<col;j++)
            {
                System.out.printf("%10s",trainData[i][j]+" ");
            }
            System.out.println();
        }
        System.out.println();
    }

    public void printTheta()
    {
        System.out.print("{");
        int i;
        for (i=0;i<theta.length-1;i++)
            System.out.print(String.format("%.2f",theta[i])+" ,");
        System.out.print(String.format("%.2f",theta[i])+"},");
    }
}
