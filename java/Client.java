import java.io.*;
import java.net.*;
import java.util.InputMismatchException;
import java.util.Scanner;

public class Client
{
    /********************* Connection Settings ***********************/

            private static final int PORT = 4322;
            private static final String IP_ADDRESS = "localhost";

    /*****************************************************************/

    //  Main Method
    public static void main(String[] args)
    {
        // POI to be sent to Master.
        Poi poi;
        /*
         *  u: user id.
         *  k: number of POIs.
         */
        int u, k;
        //  Recommendations.
        Poi[] recs;
        Scanner sc = new Scanner(System.in);
        Socket request = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        //  Dummy data.
        poi = new Poi(13, "Apostolos Nikolaidis Stadium", 37.987228, 23.754152, "Sports Venue");

        try
        {
            //  Connecting to Master.
            request = new Socket(IP_ADDRESS, PORT);
            System.out.println("Connected to Server.\n");

            //  Initializing streams.
            out = new ObjectOutputStream(request.getOutputStream());
            in = new ObjectInputStream(request.getInputStream());

            System.out.println(EXIT);

            //  Reading input.
            while(true)
            {
                try
                {
                    System.out.print(USR_PROMPT);
                    u = sc.nextInt();

                    //  Check exit case.
                    if (u == -1)
                    {
                        in.close();
                        out.close();
                        request.close();
                        System.exit(0);
                    }

                    System.out.print(POI_PROMPT);
                    k = sc.nextInt();

                    if (u < 0 || k <= 0)
                        System.out.println(INVALID_INPUT);
                    else
                        break;
                }   //  Input can't be parsed.
                catch(InputMismatchException imExc)
                {
                    System.out.println(INVALID_INPUT);
                    //  Emptying buffer.
                    sc.nextLine();
                }
            }

            //  Send data to Master.
            out.writeInt(u);
            out.writeObject(poi);
            out.writeInt(k);
            out.flush();

            //  Receive recommendations.
            recs = (Poi[]) in.readObject();

            if (recs == null)   //  u or k are too big.
                System.out.println(OUT_OF_BOUNDS);
            else
            {   //  Print recommendations.
                System.out.print(RESULTS);
                for(int i = 0; i < recs.length; i++)
                    System.out.print( " | " + recs[i].getId());
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
        finally
        {
            try
            {
                //  Closing streams.
                sc.close();
                in.close();
                out.close();
                request.close();
            }
            catch(IOException ioExc)
            {
                ioExc.printStackTrace();
            }

        }
    }

    /************************* Output Messages ***************************/

    private static final String OUT_OF_BOUNDS = "\nOut of bounds.";
    private static final String RESULTS = "Recommended POIs: ";
    private static final String EXIT = "Enter id = -1 to exit.";
    private static final String INVALID_INPUT = "\nInvalid input\n";
    private static final String USR_PROMPT = "Enter user id: ";
    private static final String POI_PROMPT = "Enter number of POIs: ";
}