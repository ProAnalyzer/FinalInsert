import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

// IF private static final int CONSUMER_COUNT = 1, Total elapsed time: 1381 ms HERE ONLY ONE THREAD IS RUNNING
// IF private static final int CONSUMER_COUNT = 4, Total elapsed time: 1545 ms ms HERE MULTI THREAD(4) IS RUNNING

public class parallelInsertInDatabaseUsingMultiThread {

    private static final int CONSUMER_COUNT = 1;

    public static void main(String[] args) {

        long startTime = System.nanoTime();

        ExecutorService producerPool = Executors.newFixedThreadPool(4);
        producerPool.submit(new Java8StreamRead(false)); // run method is
        // called

        // create a pool of consumer threads to parse the lines read
        ExecutorService consumerPool = Executors.newFixedThreadPool(Java8StreamRead.CONSUMER_COUNT);
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            consumerPool.submit(new Java8StreamRead(true)); // run method is
            // called
        }

        producerPool.shutdown();
        consumerPool.shutdown();

        while (!producerPool.isTerminated() && !consumerPool.isTerminated()) {
        }
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms");
    }


    public static class Java8StreamRead implements Runnable {

        private static final int CONSUMER_COUNT = 1;
        private final static BlockingQueue<String> linesReadQueue = new ArrayBlockingQueue<String>(50);

        private boolean isConsumer = false;
        private static boolean producerIsDone = false;

        public Java8StreamRead(boolean consumer) {
            this.isConsumer = consumer;
        }


        private void readFile() {
            try
            {
                Path file = Paths.get("C:\\Users\\User\\IdeaProjects\\FinalInsert\\EmpFinal.csv");
                //Java 8: Stream class
                Stream<String> lines = Files.lines( file, StandardCharsets.UTF_8 ).skip(1);

                for( String line : (Iterable<String>) lines::iterator )
                {
                    System.out.println("read=" + line);
                    linesReadQueue.put(line); //blocked if reaches its capacity, until consumer consumes
                    System.out.println(Thread.currentThread().getName() + ":: producer count = "+  linesReadQueue.size());
                }

            } catch (Exception e){
                e.printStackTrace();
            }

            producerIsDone = true; // signal consumer
            System.out.println(Thread.currentThread().getName() + " producer is done");
        }

        @Override
        public void run() {
            if (isConsumer) {
                consume();
            } else {
                readFile(); //produce data by reading a file
            }
        }

        private void consume() {
            try {
                String jdbcURL = "jdbc:postgresql://localhost:5432/";
                String username = "postgres";
                String password = "postgres";
                Connection connection =null;
                connection = DriverManager.getConnection(jdbcURL, username, password);
                String sql = "INSERT INTO EmployeeEngineer " +
                        "(First_Name,Middle_Name,Last_Name,Age_of_Emp,Salary,Email,Phone_Number,Address,Description,Country) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?)";
                PreparedStatement statement = connection.prepareStatement(sql);
                connection.setAutoCommit(false);
                while (!producerIsDone || (producerIsDone && !linesReadQueue.isEmpty())) {
                    String lineToProcess = linesReadQueue.take();
                    processCpuDummy(); // some CPU intensive processing
                    System.out.println("procesed:" + lineToProcess);
                    System.out.println(Thread.currentThread().getName() + " consumer is done");

                    String[] data = lineToProcess.split(",");
                    String First_Name = data[0];
                    String Middle_Name  = data[1];
                    String Last_Name  = data[2];
                    String Age_of_Emp = data[3];
                    String Salary = data[4];
                    String Email = data[5];
                    String Phone_Number = data[6];
                    String Address = data[7];
                    String Description = data[8];
                    String Country = data.length == 10 ? data[9] : "";


                    statement.setString(1,First_Name );
                    statement.setString(2, Middle_Name);
                    statement.setString(3, Last_Name);

                    int sqlAgeOfEmp = Integer.parseInt(Age_of_Emp);
                    statement.setInt(4, sqlAgeOfEmp);

                    float sqlSalary = Float.parseFloat(Salary);
                    statement.setFloat(5, sqlSalary);

                    statement.setString(6, Email);

                    int sqlPhoneNumber = Integer.parseInt(Phone_Number);
                    statement.setInt(7, sqlPhoneNumber);

                    statement.setString(8, Address);
                    statement.setString(9, Description);
                    statement.setString(10, Country);

                    statement.addBatch();

            }
                statement.executeBatch();
                connection.commit();
                connection.close();

            }catch (InterruptedException e){
                System.out.println(System.err);
            }
            catch (SQLException e){
                System.out.println(System.err);
            }


        }

        public void processCpuDummy() {
            //takes ~ 15 ms of CPU time
            //did not use Thread.sleep() as it does not consume any CPU cycles
            for (long i = 0; i < 100000000l; i++) {
                i = i+1;
            }
        }

    }

}
