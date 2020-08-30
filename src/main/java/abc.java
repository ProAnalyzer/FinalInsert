import javafx.concurrent.Task;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class abc {
    public static class Task implements Runnable{
        public void run(){

            try{
                String jdbcURL = "jdbc:postgresql://localhost:5432/";
                String username = "postgres";
                String password = "postgres";

                String csvFilePath = "EmpFinal.csv";

                Connection connection = null;
                connection = DriverManager.getConnection(jdbcURL, username, password);
                System.out.println("Connected to DataBase !");
                try {
                    String sql = "INSERT INTO EmployeeEngineer " +
                            "(First_Name,Middle_Name,Last_Name,Age_of_Emp,Salary,Email,Phone_Number,Address,Description,Country) " +
                            "VALUES (?,?,?,?,?,?,?,?,?,?)";
                    PreparedStatement statement = connection.prepareStatement(sql);
                    BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
                    String lineText ;
                    while ((lineText = lineReader.readLine()) != null) {
                        String[] data = lineText.split(",");
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

                    lineReader.close();

                    // execute the remaining queries
                    statement.executeBatch();

                    connection.commit();
                    connection.close();
                }catch (SQLException e){
                    System.out.println(System.err);
                }catch (FileNotFoundException e){
                    e.printStackTrace();
            }
            }catch (SQLException e){
                System.out.println(System.err);
            }
            catch (IOException e){
                e.printStackTrace();

            }

            }
        }
    public static void main(String[] args) throws  Exception{
        /*final int threadSize = 4;
        System.out.println(LocalTime.now());
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadSize);
        Task task = new Task();
        executor.execute(task);*/


    }
}
