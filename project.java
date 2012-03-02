import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

public class project {

	public static void main(String[] args) {

		Connection con = null;

		try {
			// Get the connection to the database
			DriverManager.registerDriver(new SQLServerDriver());
			String url = "jdbc:sqlserver://ACS-CSEB-SRV.ucsd.edu:1433;databaseName="
			+ args[0];
			con = DriverManager.getConnection(url, args[0], args[1]);

			// Query the Flights table
			Statement stmt = con.createStatement();
			Statement stmt2 = con.createStatement();
			Statement stmt3 = con.createStatement();
			Statement stmt4 = con.createStatement();
			try {
				stmt.executeUpdate("create table TClosure(Origin char(32), Destination char(32), stops integer)");
				stmt.executeUpdate("create table delta(origin char(32), destination char(32), stops integer)");
			}
			catch(SQLException e) {

			}
			stmt.executeUpdate("Insert into TClosure (Origin, Destination) SELECT * FROM " + args[2]);
			stmt.executeUpdate("Insert into delta (Origin, Destination) SELECT * FROM " + args[2]);

			ResultSet rset = stmt2.executeQuery("Select * from delta");
			ResultSet counter = stmt3.executeQuery("SELECT count(*) as count from delta");
			//stmt.executeUpdate("drop table TClosureold");

			int i=0;
			System.out.println("Before loop");
			while(rset.next()) {
				System.out.println("In while loop...");
				try {
					stmt.executeUpdate("create table TClosureold (Origin char(32), Destination char(32), stops integer)");
				}
				catch(SQLException e) {}
				stmt.executeUpdate("Insert into TClosureold (Origin, Destination) SELECT * FROM " + args[2]);
				stmt.executeUpdate("TRUNCATE TABLE TClosure");
				stmt.executeUpdate("INSERT INTO TClosure (Origin, Destination) ((SELECT Origin, Destination from TClosureold) UNION (SELECT x.Origin, y.Destination from delta x, TClosureold y where x.Destination = y.Origin and x.Origin <> y.destination) UNION (SELECT x.Origin, y.Destination from TClosureold x, delta y where x.Destination = y.Origin and x.Origin <> y.Destination))");
				stmt.executeUpdate("TRUNCATE table delta");
				stmt.executeUpdate("Insert into delta (Origin, Destination) (SELECT T.Origin, T.Destination FROM TClosure T LEFT JOIN TClosureold Old ON Old.Origin = T.Origin AND Old.Destination = T.Destination and Old.Origin <> T.Destination)");
				//stmt.executeUpdate("Insert into delta (Origin, Destination) values ('a', 'b')");
				//rset.close();
				//rset = stmt2.executeQuery("SELECT * from delta");
				stmt.executeUpdate("drop table TClosureold");

			}

			System.out.println("After Loop");
			ResultSet contents = stmt4.executeQuery("SELECT * from TClosure ORDER BY Origin, Destination");
			// Print the Origin and Destination columns
			
			while (contents.next()) {
				System.out.print(contents.getString("Origin"));
				System.out.print("---");
				System.out.print(contents.getString("Destination"));
				System.out.print("---");
				System.out.println(contents.getString("stops"));
			}
			contents.close();

			// close the result set, statement

			//rset.close();
			stmt.executeUpdate("drop table TClosure");
			stmt.executeUpdate("drop table delta");
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException("There was a problem!", e);
		} finally {
			// I have to close the connection in the "finally" clause otherwise
			// in case of an exception i would leave it open.
			try {
				if (con != null)
					con.close();
			} catch (SQLException e) {
				throw new RuntimeException(
					"Help! I could not close the connection!!!", e);
			}
		}
	}
}


