import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.*;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;

public class Master
{
    /************************ Settings **************************/

            //  Number of Workers.
            private static final int NODES = 4;
            //  Row and column dimensions.
            private static final int ROWS = 15;    //  765 |  5
            private static final int COLS = 30;   //  1964 | 30
            //  Filename of data file.
            private static final String filename = "sample1.csv";
            //  Connection settings.
            private static final int BACKLOG = 10;
            private static final int W_PORT = 4321;     //  Port used by workers.
            private static final int C_PORT = 4322;     //  Port used by clients.

    /************************************************************/

    /*
     *  total: the total weight of the workers.
     *  The total variable is used in load balancing.
     *  last: previous calculated value of the cost function.
     */
    private double total, last;
    private ExecutorService pool;                          //  Thread pool for the threads handling the communication with the workers.
    private ServerSocket provider;
    private RealMatrix c, p, y, x;
    private OpenMapRealMatrix r;                           //  The input matrix.
    private ArrayList<Master.Worker> list;                 //  The list containing all the workers.
    private ArrayList<Future<RealMatrix>> results;         //  The list where the result returned by each Worker is stored.
    private ArrayList<Callable<RealMatrix>> callables;     //  The list where all the threads are stored.

    //  Constructor.
    public Master() { initialize(); }

    //  Initialization method.
    private void initialize()
    {
        //  Checking parameters.
        if (NODES <= 0 || ROWS <= 0 || COLS <= 0)
        {
            System.out.println("Invalid parameters.");
            System.exit(0);
        }

        //  Initializing variables.
        total = 0;
        list = new ArrayList<Master.Worker>();
        results = new ArrayList<Future<RealMatrix>>();
        callables =  new ArrayList<Callable<RealMatrix>>();
        pool = Executors.newFixedThreadPool(NODES);

        r = new OpenMapRealMatrix(ROWS, COLS);

        //  Parsing .csv file.
        try
        {
            String[] values;
            File file = new File("src\\main\\resources\\" + filename);
            System.out.println(file.getAbsolutePath());
            Scanner input = new Scanner(file);

            while(input.hasNext())
            {
                values = input.nextLine().split(",");
                r.setEntry(Integer.parseInt(values[0].trim()), Integer.parseInt(values[1].trim()), Double.parseDouble(values[2].trim()));
            }
        }
        catch(FileNotFoundException fnfExc)
        {
            fnfExc.printStackTrace();
            System.exit(-1);
        }

        //  Initializing C, P matrices.
        c = MatrixUtils.createRealMatrix(r.getRowDimension(), r.getColumnDimension());
        p = MatrixUtils.createRealMatrix(r.getRowDimension(), r.getColumnDimension());

        calculateCPMatrix();

        //  The row dimension of the x,y matrices.
        int k = (Math.max(r.getRowDimension(), r.getColumnDimension())) / 4;
        k = (k == 0) ? 1 : k;       //  At least 1.

        // Initializing X, Y matrices.
        y = MatrixUtils.createRealMatrix(r.getRowDimension(), k);
        x = MatrixUtils.createRealMatrix(r.getColumnDimension(), k);

        RandomGenerator generator = new JDKRandomGenerator();

        //  Initializing X & Y matrices.
        for(int i = 0; i < y.getRowDimension(); i++)
            for(int j = 0; j < y.getColumnDimension(); j++)
                y.setEntry(i, j, generator.nextDouble());

        for(int i = 0; i < x.getRowDimension(); i++)
            for(int j = 0; j < x.getColumnDimension(); j++)
                x.setEntry(i, j, generator.nextDouble());

        //  Calculating cost/
        last = cost();
    }

    /*
     *  Partitions the matrix to be calculated (X or Y) and initializes the threads.
     *  Returns the starting row of the next call.
     *  start: the row form which the partitioning starts.
     *  current: which matrix is being sent to the workers. If current == X then Y is calculated.
     *  index: the index of the Worker in the list.
     */
    private int partition(int start, RealMatrix current, int index)
    {
        int end;
        boolean turn;       //  Shows which matrix is being sent. If current == X then turn == true.
        RealMatrix c, p;    //  These matrices are submatrices of the original C & P to be used by the workers.
        int[] dimensions = new int[2];      //  The dimensions to be used by the worker.
        Master.Worker w = list.get(index);

        p = (current == x) ? y : x;

        if (start >= p.getRowDimension())   //  If the workers are more than the total rows, only those needed are used.
            return start;

        /*
         *  Each worker has a "weight", which is a function of its available memory and CPU cores.
         *  Each worker calculates the percentage of rows corresponding the percentage of its weight
         *  to the total weight of all the workers.
         */
        if (index < list.size() - 1)
            end = (int) Math.round((w.weight / total) * p.getRowDimension()) + start;
        else
            end = p.getRowDimension();      // If it's the last worker, it calculates all the remaining rows.

        dimensions[0] = end - start;
        dimensions[1] = current.getColumnDimension();

        if (current == x)   //  Initializing the corresponding C & P submatrices
        {
            c = this.c.getSubMatrix(start, end - 1, 0, this.c.getColumnDimension() - 1);
            p = this.p.getSubMatrix(start, end - 1, 0, this.p.getColumnDimension() - 1);
        }
        else
        {
            c = this.c.getSubMatrix(0, this.c.getRowDimension() - 1, start, end - 1);
            p = this.p.getSubMatrix(0, this.p.getRowDimension() - 1, start, end - 1);
        }

        turn = (current == x);

        //  Adding the thread to the list. It will be executed later.
        callables.add(index, new Master.WorkerThread(w, c, p, current, turn, dimensions));

        return end;
    }

    //  Marges the results calculated by each worker to one matrix.
    private void merge(RealMatrix current)
    {
        try
        {
            /*
             *  tmp: the data of the merged matrix.
             *  aux1, aux2: auxiliary matrices.
             */
            double[][] tmp = results.get(0).get().getData(), aux1, aux2;

            for (int i = 1; i < results.size(); i++)
            {
                aux1 = results.get(i).get().getData();
                aux2 = new double[tmp.length + aux1.length][tmp[0].length];

                for (int k = 0; k < tmp.length + aux1.length; k++)
                {
                    for (int l = 0; l < tmp[0].length; l++)
                    {
                        if (k < tmp.length)
                            aux2[k][l] = tmp[k][l];
                        else
                            aux2[k][l] = aux1[k - tmp.length][l];
                    }
                }
                tmp = aux2;
            }

            if (current == x)
                y = MatrixUtils.createRealMatrix(tmp);
            else
                x = MatrixUtils.createRealMatrix(tmp);

        }
        catch (InterruptedException iExc)
        {
            iExc.printStackTrace();
        }
        catch (ExecutionException eExc)
        {
            eExc.printStackTrace();
        }
    }

    private void openServer()
    {
        int k;
        Thread t;
        Master.Worker tmp;
        Socket connection;
        ObjectInputStream in;
        RealMatrix current = x;

        try
        {
            provider = new ServerSocket(W_PORT, BACKLOG);       //  Opening Master.

            System.out.println("Awaiting workers...\n");
            while (list.size() < NODES)       //  Waiting for all Workers.
            {
                connection = provider.accept();
                in = new ObjectInputStream(connection.getInputStream());
                //  Creating Worker object.
                tmp = new Master.Worker(in, new ObjectOutputStream(connection.getOutputStream()), connection, (long[]) in.readObject());
                //  Updating total weight.
                total += tmp.weight;
                //  Adding Worker to the list.
                list.add(tmp);
                System.out.printf("Worker %d connected.\n", list.size());
            }

            //  Begin calculation.
            while (true)
            {
                //  Switching matrix to be sent (X or Y).
                current = (current == x) ? y : x;

                System.out.printf("Calculating %c.\n", (current == x) ? 'y' : 'x');

                /*
                 *  Partitioning the matrix.
                 *  The end of a call is the start of the next.
                 *  Starting from 0.
                 */
                k = 0;
                for(int i = 0; i < list.size(); i++)
                    k = partition(k, current, i);

                //  Running the threads and block till they all end.
                results = (ArrayList) pool.invokeAll(callables);

                //  Merging the results to a matrix.
                merge(current);

                //  Clearing results and threads for the next calculation.
                callables.clear();
                results.clear();

                //  If both X & Y are calculated check the error.
                if (current == x)
                    if (error() < THRESHOLD)
                        break;
            }

            //  Creating termination threads, using the termination constructor.
            for(int i = 0; i < list.size(); i++)
                callables.add(new WorkerThread(list.get(i)));

            //  Terminating Workers and shutting down thread pool.
            pool.invokeAll(callables);
            pool.shutdown();

            //  Terminating communication with Workers.
            provider.close();

            //  Awaiting clients.
            System.out.println("Awaiting clients...\n");
            provider = new ServerSocket(C_PORT, BACKLOG);

            while(true)
            {
                connection = provider.accept();
                System.out.println("Client connected.");

                //  Creating request handler.
                t = new Handler(connection);

                t.start();
            }
        }
        catch (IOException ioExc)
        {
            ioExc.printStackTrace();
        }
        catch (ClassNotFoundException cnfExc)
        {
            cnfExc.printStackTrace();
        }
        catch(InterruptedException iExc)
        {
            iExc.printStackTrace();
        }
        finally
        {
            try
            {
                //  Closing server.
                provider.close();
            }
            catch(IOException ioExc)
            {
                ioExc.printStackTrace();
            }
        }
    }

    //  Calculates the C & P matrices.
    private void calculateCPMatrix()
    {
        double e;

        for (int i = 0; i < r.getRowDimension(); i++)
            for (int j = 0; j < r.getColumnDimension(); j++)
            {
                e = r.getEntry(i, j);
                c.setEntry(i, j, 1 + ALPHA * e);
                p.setEntry(i, j, (e > 0) ? 1 : 0);
            }
    }

    //  Calculated the score of poi i for a given user u.
    private synchronized double getScore(int u, int i)
    {
        RealVector xu = x.getRowVector(u);
        RealVector yi = y.getRowVector(i);

        return yi.dotProduct(xu);
    }

    //  Returns the recommendations.
    private synchronized Poi[] getRecommendations(int u, int k, Poi poi)
    {
        //  poi: not used at this stage.
        Poi[] pois;
        ArrayList<Pair> list;

        /*
         *  Checking the parameters.
         *  k is not a coordinate but multitude so equality is allowed.
         */
        if (u >= r.getColumnDimension() || k > r.getRowDimension())
            return null;

        //  Contains the values of each poi for a given user.
        list = new ArrayList<Pair>(r.getRowDimension());

        for(int id = 0; id < r.getRowDimension(); id++)
            if (poi.getId() != id)
                list.add(new Pair(id, getScore(u, id)));    //  Adding the Pairs.

        Collections.sort(list);     //  The Pair class implements the Comparable interface.

        pois = new Poi[k];

        //  The sorting is in ascending order. We need descending.
        for(int q = 0; q < pois.length; q++)                        //  Dummy data.
            pois[q] = new Poi(list.get(list.size() - q - 1).id, "The Great Rift", Math.E, Math.PI, "Dark Nebulae") ;

        return pois;
    }

    //  Calculated the cost function.
    private double cost()
    {
        RealVector xu, yi;
        /*
         *  done: is false only during the first execution of the inner loop.
         *  It helps calculate sumy only once.
         */
        boolean done = false;
        //  sumx, sumy: the sums used in the regularization term.
        double tmp, sumx = 0.0, sumy = 0.0, sum = 0.0;

        for(int u = 0; u < x.getRowDimension(); u++)
        {
            xu = x.getRowVector(u);
            sumx += Math.pow(xu.getNorm(), 2);
            for (int i = 0; i < y.getRowDimension(); i++)
            {
                yi = y.getRowVector(i);

                if (!done)
                    sumy += Math.pow(yi.getNorm(), 2);

                tmp = Math.pow(p.getEntry(i, u) - yi.dotProduct(xu), 2);
                tmp *= c.getEntry(i, u);

                sum += tmp;
            }
            done = true;
        }

        return sum + LAMBDA*(sumx + sumy);
    }

    //  Calculates the error between two calculations.
    private double error()
    {
        double error;
        double tmp = last;
        double cost = cost();

        last = cost;

        error = Math.abs(cost - tmp);
        System.out.printf("Error: %f\n", error);
        System.out.println("========================");

        return error;
    }

    //  Main method
    public static void main(String[] args) { new Master().openServer(); }

    /*****************************************************************************************/

    //  This class is used to store information about the Workers, it's used as a struct.
    private static class Worker
    {
        long[] specs;       //  CPU cores and available memory.
        double weight;      //  The weight of the Worker.
        //  The socket and the IO streams of the Worker.
        Socket connection;
        ObjectInputStream in;
        ObjectOutputStream out;

        //  Constructor.
        Worker(ObjectInputStream in, ObjectOutputStream out, Socket connection, long[] specs)
        {
            this.in = in;
            this.out = out;
            this.specs = specs;
            this.connection = connection;
            weight = CPU_WEIGHT*specs[CPU] + MEM_WEIGHT*specs[MEM];
        }
    }

    /*****************************************************************************************/

    // The Pair class is used in producing the recommendations.
    private static class Pair implements Comparable<Pair>
    {
        int id;         //  The id of the object. In the next stage this field will be of type POI.
        double val;     //  The value of the object.

        //  Constructor.
        Pair(int id, double val)
        {
            this.id = id;
            this.val = val;
        }

        //  The comparison is based on the value of the val field.
        public int compareTo(Pair p)
        {
            if (this.val > p.val)
                return 1;
            else if (this.val == p.val)
                return 0;

            return -1;
        }
    }

    /*****************************************************************************************/

    //  The WorkerThread handles the communication with Worker.
    private static class WorkerThread implements Callable<RealMatrix>
    {
        //  Data to be sent to Workers.
        int[] dimensions;
        boolean turn, done;
        private Socket connection;
        private RealMatrix c, p, m;
        //  IO streams.
        private ObjectInputStream in;
        private ObjectOutputStream out;

        //  This constructor is used to terminate a worker.
        WorkerThread(Worker w)
        {
            this.done = true;   //  No more calculations.
            this.in = w.in;
            this.out = w.out;
            this.connection = w.connection;
        }

        //  This one is used when we are not done yet.
        WorkerThread(Worker w, RealMatrix c, RealMatrix p, RealMatrix m, boolean turn, int[] dimensions)
        {
            this.c = c;
            this.p = p;
            this.m = m;
            this.turn = turn;
            this.done = false;  //  There are still calculations remaining.
            this.dimensions = dimensions;
            this.in = w.in;
            this.out = w.out;
            this.connection = w.connection;
        }

        // Implementing Callable<T> interface.
        public RealMatrix call()
        {
            RealMatrix a = null;

            try
            {
                out.writeBoolean(done);

                //  If we are done, socket and streams are closed.
                if (done)
                {
                    out.flush();
                    out.close();
                    in.close();
                    connection.close();
                }
                else    // else, send the data to the client & read the result.
                {
                    out.writeObject(dimensions);
                    out.writeBoolean(turn);
                    out.writeObject(c);
                    out.writeObject(p);
                    out.writeObject(m);
                    out.flush();
                    a = (RealMatrix) in.readObject();
                }
            }
            catch (IOException ioExc)
            {
                ioExc.printStackTrace();
            }
            catch(ClassNotFoundException cnfExc)
            {
                cnfExc.printStackTrace();
            }
            finally
            {
                //  This value is added to the Future<RealMatrix> list.
                return a;
            }
        }
    }

    /*****************************************************************************************/

    // The Handler class handles the client requests.
    private class Handler extends Thread
    {
        private Poi poi;    //  POI received by the client.
        /*
         *  k: multitude of POIs to recommend.
         *  u: user id.
         */
        private int k, u;
        private Poi[] recs;     // Recommendations for the client.
        private Socket connection;

        public Handler(Socket connection) { this.connection = connection; }

        //  Extending Thread class.
        public void run()
        {
            ObjectInputStream in = null;
            ObjectOutputStream out = null;

            try
            {
                //  Initializing streams.
                in = new ObjectInputStream(connection.getInputStream());
                out = new ObjectOutputStream(connection.getOutputStream());

                //  Reading input.
                u = in.readInt();       //  User id.
                poi = (Poi) in.readObject();    //  POI.
                k = in.readInt();       //  Number of POIs.

                //  Producing recommendations.
                recs = getRecommendations(u, k, poi);

                //  Sending recommendations to client.
                out.writeObject(recs);
                out.flush();

                System.out.println("Request handled.");
            }
            catch(IOException ioExc)
            {
                ioExc.printStackTrace();
            }
            catch(ClassNotFoundException cnfExc)
            {
                cnfExc.printStackTrace();
            }
            finally
            {
                try
                {
                    //  Closing streams and socket.
                    in.close();
                    out.close();
                    connection.close();
                }
                catch(IOException ioExc)
                {
                    ioExc.printStackTrace();
                }
            }
        }
    }

    /*************************************** Constants ***************************************/

    //  Indexes for the rec[] array.
    private static final int CPU = 0;
    private static final int MEM = 1;
    //  α value used in creating the C matrix.
    private static final int ALPHA = 40;
    //  λ value used in regularization.
    private static final double LAMBDA = 0.5;
    //  Error margin.
    private static final double THRESHOLD = 0.1;
    //  The weight (or significance) of memory and CPU cores.
    private static final double MEM_WEIGHT = 0.4;
    private static final double CPU_WEIGHT = 1 - MEM_WEIGHT;

}