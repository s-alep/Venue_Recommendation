import java.io.*;
import java.net.*;
import org.apache.commons.math3.linear.*;

public class Worker
{
    /******************* Connection Settings *********************/

        private static final int PORT = 4321;
        private static final String IP_ADDRESS = "localhost";

    /*************************************************************/

    //  CPU and memory specs of the system.
    private long[] specs;
    //  p (column or row) vector of k user or item.
    private RealVector pk;
    private Socket request;
    //  Dimensions received from Master.
    private int[] dimensions;
    // if done == true no more computation.
    private boolean turn, done;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private RealMatrix m, mm, c, p, ck, result;

    public Worker() { initialize(); }

    //  Initialization method.
    private void initialize()
    {
        //  Initializing specs.
        specs = new long[2];
        specs[0] = Runtime.getRuntime().availableProcessors();
        specs[1] = Runtime.getRuntime().freeMemory() / (1024 * 1024);

        try
        {   //  Connecting to Master.
            request = new Socket(IP_ADDRESS, PORT);
            System.out.println("Connected to Master.\n");

            out = new ObjectOutputStream(request.getOutputStream());

            //  Sending specs to Master.
            out.writeObject(specs);
            out.flush();
        }
        catch(UnknownHostException uhExc)
        {
            uhExc.printStackTrace();
        }
        catch(IOException ioExc)
        {
            ioExc.printStackTrace();
        }
    }

    //  Closing socket and streams.
    private boolean close()
    {
        try
        {
            in.close();
            out.close();
            request.close();
        }
        catch(IOException ioExc)
        {
            ioExc.printStackTrace();
            return false;
        }
        return true;
    }

    //  Sending results to Master.
    private void sendResults() throws IOException
    {
        out.writeObject(result);
        out.flush();
    }

    //  Calculates the matrix.
    private void calculate()
    {
        try
        {
            in = new ObjectInputStream(request.getInputStream());
            //  Reading input from Master.
            done = in.readBoolean();
            //  Dimensions used in calculation.
            dimensions = (int[]) in.readObject();
            // If turn == true then m = X.``
            turn = in.readBoolean();
            c = (RealMatrix) in.readObject();
            p = (RealMatrix) in.readObject();
            m = (RealMatrix) in.readObject();

            while(true)
            {
                //  Creating result matrix.
                result = MatrixUtils.createRealMatrix(dimensions[0], dimensions[1]);
                preCalculate();


                for(int k = 0; k < dimensions[0]; k++)
                {
                    System.out.printf("Calculating row %d of %d.\n", k + 1, dimensions[0]);
                    //  Extracts the k column or row of P matrix.
                    calculatePkVector(k);
                    //  Creates the diagonal Cu or Ci matrix.
                    calculateCkMatrix(k);

                    //  Calculates the row of the result.
                    result.setRowVector(k, calculateMatrixRow());
                }

                System.out.println("========================");

                //  Sends results to Master.
                sendResults();

                //  Are we done?
                done = in.readBoolean();

                if (done)
                    break;

                //  Prepares for next calculation.
                dimensions = (int[]) in.readObject();
                turn = in.readBoolean();
                c = (RealMatrix) in.readObject();
                p = (RealMatrix) in.readObject();
                m = (RealMatrix) in.readObject();
            }
        }
        catch(IOException ioExc)
        {
            ioExc.printStackTrace();
        }
        catch(ClassNotFoundException cnfExc)
        {
            cnfExc.printStackTrace();
        }
    }

    // Extracts p row or column of matrix P.
    private void calculatePkVector(int k)
    {
        //  If turn == X then Y is being calculated.
        if (turn == X)
            pk = p.getRowVector(k);
        else
            pk = p.getColumnVector(k);
    }

    //  Creates Ck diagonal matrix.
    private void calculateCkMatrix(int k)
    {
        double[] diagonal;

        //  If turn == X then Y is being calculated.
        if (turn == X)
            diagonal = c.getRow(k);
        else
            diagonal = c.getColumn(k);

        ck = MatrixUtils.createRealDiagonalMatrix(diagonal);
    }

    //  Pre-calculation
    private void preCalculate() { mm = m.transpose().multiply(m); }

    //  The actual computation of the result matrix row.
    private RealVector calculateMatrixRow()
    {
        RealMatrix I = MatrixUtils.createRealIdentityMatrix(ck.getRowDimension());
        RealMatrix a = ck.subtract(I);

        a = mm.add(m.transpose().multiply(a.multiply(m)));
        I = MatrixUtils.createRealIdentityMatrix((a.getRowDimension()));
        a = a.add(I.scalarMultiply(LAMBDA));
        a = (new QRDecomposition(a)).getSolver().getInverse();

        RealVector v = ck.operate(pk);
        v = m.transpose().operate(v);

        return a.operate(v);
    }

    //  Main Method
    public static void main(String[] args)
    {
        Worker worker = new Worker();

        worker.calculate();

        worker.close();

        System.out.println("Worker exited.");
    }

    //  X is the X matrix.
    private static final boolean X = true;
    private static final double LAMBDA = 0.01;
}