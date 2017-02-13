import java.sql.*;
import java.util.*;
import java.lang.*;
import org.postgis.*;
import org.postgresql.util.PGobject;

import javax.sound.sampled.Line;

public class JavaGIS {

    public static void main(String[] args) {

        java.sql.Connection conn;

        try {
    /* 
    * Load the JDBC driver and establish a connection. 
    */
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/opengeo";
            conn = DriverManager.getConnection(url, "emilbonnerup", "");
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
            ResultSet r = s.executeQuery("select way,osm_id from planet_osm_line");
            while( r.next() ) {
      /* 
      * Retrieve the geometry as an object then cast it to the geometry type. 
      * Print things out. 
      */
                PGgeometry way = (PGgeometry) r.getObject(1);
                LineString lineString = (LineString) way.getGeometry();
                int id = r.getInt(2);
                System.out.println("Row " + id + ":");
                System.out.println(lineString.length());
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