import java.io.IOException;

public class T
{
// Test class for jdbc2_fdw java code
//
// Heimir Sverrisson, 2015-04-17
//
    private String a[] = {
        "org.postgresql.Driver", // Driver class name
        "jdbc:postgresql://localhost:5432/datawarehouse", // URL
        "datawarehouse", // username
        "S2mpleS2mple", // password
        "15", // querytimeout (seconds)
        "/usr/local/jars/postgresql-9.4-1201.jdbc41.jar" // jarfile
    };
    private JDBCUtils jdbcUtils;

    // Public constructor
    public T(){
        jdbcUtils = new JDBCUtils();
    }

    // The method that calls JNI createConnection
    private String createConnection() throws IOException{
        return jdbcUtils.createConnection(a);
    }

    // Create statement
    private String createStatement(String query) throws IOException{
        return jdbcUtils.createStatement(query);
    }

    private void iterate(){
        String res[];
        while((res = jdbcUtils.returnResultSet()) != null){
            for(int i=0; i < res.length; i++){
                System.out.print(res[i] + '\t');
            }
            System.out.println();
        }
    }

    private String closeStatement(){
        return jdbcUtils.closeStatement();
    }

    private String closeConnection(){
        return jdbcUtils.closeConnection();
    }

    private void report(String s, String operation){
        if(s == null){
            System.out.println("Successful " + operation);
        } else {
            System.out.println(operation + " throws exception:\n" + s);
        }
    }

    private static String query1 = "SELECT * from staging.sta_utilities";
    private static String query2 = "SELECT full_name,name,id,current_timestamp from staging.sta_utilities";

    public static void main(String args[]) throws IOException{
        T t = new T();
        t.report(t.createConnection(), "createConnection");
        t.report(t.createStatement(query1), "createStatement");
        t.iterate();
        t.report(t.closeStatement(), "closeStatement");
        t.report(t.createStatement(query2), "createStatement");
        t.iterate();
        t.report(t.closeConnection(), "closeConnection");
        t.report(t.createStatement(query2), "createStatement");
        System.exit(0);
    }
}
