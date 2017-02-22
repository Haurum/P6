import java.sql.*;
import java.util.*;
import java.lang.*;
import java.util.stream.LongStream;

import com.sun.deploy.security.ValidationState;
import org.postgis.*;
import org.postgresql.util.PGobject;
import sun.jvm.hotspot.runtime.ResultTypeFinder;
import org.postgresql.util.PSQLException;

import javax.sound.sampled.Line;

public class JavaGIS {

    public static void main(String[] args) {
        //populateEdgesWithLinestrings();
        //createEdgesFromWays();
    }

    static void populateEdgesWithLinestrings(){
        java.sql.Connection conn;
        try{
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/test";
            conn = DriverManager.getConnection(url, "postgres", "");
            Statement s = conn.createStatement();
            ResultSet r = s.executeQuery("SELECT * FROM edge");
            ArrayList<EdgeTemp> edges = new ArrayList<>();
            while(r.next()){
                EdgeTemp e = new EdgeTemp();

                e.id = r.getLong("id");
                Long[] a = (Long[]) r.getArray("nodeids").getArray();
                e.nodeIds = new ArrayList<Long>(Arrays.asList(a));
                edges.add(e);
            }
            for(EdgeTemp edge : edges){
                ArrayList<Point> nodePoints = new ArrayList<>();
                for(Long nodeId : edge.nodeIds){
                    ResultSet result = s.executeQuery("SELECT * FROM nodes WHERE nodes.id = " + nodeId);
                    if(result.next()){
                        PGgeometry way = (PGgeometry) result.getObject("geom");
                        Point p = (Point) way.getGeometry();
                        nodePoints.add(p);
                    }else{
                        throw new Exception("node not found");
                    }
                }
                Point[] nodePointsArray = new Point[nodePoints.size()];
                nodePointsArray = nodePoints.toArray(nodePointsArray);
                edge.lineString = new LineString(nodePointsArray);
                edge.lineString.setSrid(edge.lineString.getFirstPoint().srid);
                String sql = "UPDATE edge SET linestring=? WHERE id=?";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setObject(1, new PGgeometry(edge.lineString));
                pstmt.setLong(2, edge.id);
                System.out.println(edge.id);
                pstmt.executeUpdate();
            }
            s.close();
            conn.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void createEdgesFromWays(){
        java.sql.Connection conn;
        long globalId = 1;
        try {
    /*
    * Load the JDBC driver and establish a connection.
    */
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/test";
            conn = DriverManager.getConnection(url, "postgres", "");
    /*
    * Create a statement and execute a select query.
    */
            Statement s = conn.createStatement();
            ResultSet r = s.executeQuery("SELECT node_id FROM way_nodes GROUP BY node_id HAVING COUNT(node_id)>1");
            ArrayList<Long> nodeIds = new ArrayList<>();
            ArrayList<EdgeTemp> edges = new ArrayList<>();
            while (r.next()) {
      /*
      * Retrieve the geometry as an object then cast it to the geometry type.
      * Print things out.
      */
                //PGgeometry way = (PGgeometry) r.getObject(1);
                //LineString lineString = (LineString) way.getGeometry();
                long id = r.getLong(1);
                //System.out.println(id);// + " | " + r.getLong(2) + " | " + r.getArray(3));
                nodeIds.add(id);
            }
            r = s.executeQuery("SELECT * FROM ways");
            while (r.next()){
                EdgeTemp e = new EdgeTemp();
                e.id = globalId;
                globalId++;
                e.wayId = r.getLong("id");
                e.parent = null;
                Long[] a = (Long[]) r.getArray("nodes").getArray();
                e.nodeIds = new ArrayList<>(Arrays.asList(a));
                edges.add(e);
            }
            int count;
            do{
                count = 0;
                for (long nodeId : nodeIds) {
                    ArrayList<EdgeTemp> newEdges = new ArrayList<>();
                    for (EdgeTemp edge : edges) {
                        int index = edge.nodeIds.indexOf(nodeId);
                        if (index > 0 && index < edge.nodeIds.size() - 1) {
                            count++;
                            ArrayList<Long> firstNodes = new ArrayList<>(edge.nodeIds.subList(0, index + 1));
                            ArrayList<Long> lastNodes = new ArrayList<>(edge.nodeIds.subList(index, edge.nodeIds.size()));
                            EdgeTemp e1 = new EdgeTemp();
                            e1.wayId = edge.wayId;
                            e1.id = globalId;
                            globalId++;
                            e1.parent = edge;
                            e1.nodeIds = firstNodes;
                            newEdges.add(e1);
                            EdgeTemp e2 = new EdgeTemp();
                            e2.wayId = edge.wayId;
                            e2.id = globalId;
                            globalId++;
                            e2.parent = edge;
                            e2.nodeIds = lastNodes;
                            newEdges.add(e2);
                        }
                    }
                    for (EdgeTemp newEdge : newEdges) {
                        edges.remove(newEdge.parent);
                        edges.add(newEdge);
                    }
                }
            }while (count > 0);
            for(EdgeTemp e : edges){
                Array nodes = conn.createArrayOf("BIGINT", e.nodeIds.toArray());
                String sql = "INSERT INTO edge (way_id, nodeids) VALUES (?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);

                pstmt.setLong(1, e.wayId);
                pstmt.setArray(2, nodes);
                pstmt.executeUpdate();

            }
            s.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void createEdgeNodes()
    {
        java.sql.Connection conn;
        try{
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/postgres";
            conn = DriverManager.getConnection(url, "postgres", "");

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class EdgeTemp {
    EdgeTemp parent;
    long id;
    long wayId;
    ArrayList<Long> nodeIds;
    LineString lineString;

}