import java.sql.*;
import java.util.*;
import java.lang.*;
import org.postgis.*;
import org.postgresql.util.PGobject;
import sun.jvm.hotspot.runtime.ResultTypeFinder;

import javax.sound.sampled.Line;

public class JavaGIS {

    public static void main(String[] args) {

        java.sql.Connection conn;

        try {
    /* 
    * Load the JDBC driver and establish a connection. 
    */
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/postgres";
            conn = DriverManager.getConnection(url, "postgres", "");
    /* 
    * Add the geometry types to the connection. Note that you 
    * must cast the connection to the pgsql-specific connection 
    * implementation before calling the addDataType() method. 
    */
           /*
            ((org.postgresql.PGConnection)conn).addDataType("geometry","org.postgis.PGgeometry");
            ;
            ((org.postgresql.PGConnection)conn).addDataType("box3d","org.postgis.PGbox3d");
            */
    /* 
    * Create a statement and execute a select query.
    */
            Statement s = conn.createStatement();
            ResultSet r = s.executeQuery("select * from edge");

            while( r.next() ) {
                System.out.println("Row " + r.getArray(1));
                Long edge_id = r.getLong(8);

                Long[] arr = (Long[]) r.getArray(1).getArray();
                int seqNr = 0;
                for (long nodeId : arr) {
                    String sql = "INSERT INTO edge_nodes (edge_id, node_id, sequence_id) VALUES (?, ?, ?)";
                    PreparedStatement pstmt = conn.prepareStatement(sql);
                    pstmt.setLong(1, edge_id);
                    pstmt.setLong(2, nodeId);
                    pstmt.setInt(3, seqNr);
                    pstmt.executeUpdate();
                    seqNr++;
                }
            }
            s.close();
            conn.close();
        }

        catch(ClassCastException e){
            e.printStackTrace();
        }

        catch(Exception e ) {
            e.printStackTrace();
        }
    }
}