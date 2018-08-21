package com.main;

/**
 * Wine tasting Problem
 * Author: khushbu Kumari
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class DoTaskMySQL {
	static final String inputFilename = "person_wine_3.txt"; /* input file path */
	static final String outputFilename = "result.txt"; /* output file path */
	static final String username = "hbstudent";/* mysql schema username */
	static final String password = "hbstudent";/* mysql schema password */
	/*
	 * mysql connection URL
	 */
	static final String connectionURL = "jdbc:mysql://127.0.0.1:3306/WINETASTE?allowPublicKeyRetrieval=true&useSSL=false";
	/*
	 * query to insert weight in winerank table
	 */
	static final String wineRankQuery = "INSERT INTO WinesRank(wid, weight) SELECT wid AS 'wid', 10.0/COUNT(pid) as 'weight' FROM WishList GROUP BY wid";
	/*
	 * query to insert weight in Personrank table
	 */
	static final String personRankQuery = "INSERT INTO PersonsRank(pid, weight) SELECT WishList.pid as pid, SUM(WinesRank.weight) AS weight FROM WishList, WinesRank  WHERE (WishList.wid = WinesRank.wid) GROUP BY WishList.pid";
	/*
	 * query to select final outcome
	 */
	static final String getOutQuery = "SELECT PersonsRank.pid, WinesRank.wid FROM PersonsRank, WishList, WinesRank WHERE PersonsRank.pid = WishList.pid AND WishList.wid = WinesRank.wid ORDER BY PersonsRank.weight DESC, PersonsRank.pid ASC, WinesRank.weight DESC";
	/*
	 * Connection Object
	 */
	Connection con = null;

	/**
	 * @param con
	 */
	public DoTaskMySQL() {
		this.con = getconnection();
	}

	/*
	 * Main Controller Method
	 * 
	 **/
	public static void main(String[] args) {
		DoTaskMySQL obj = new DoTaskMySQL();
		try {
			obj.truncateQuery();
			obj.readInput();
			obj.PrepareOutputData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * reading input from the input file
	 * 
	 **/
	public void readInput() throws SQLException {
		String line;

		try (BufferedReader br = new BufferedReader(new FileReader("person_wine_3.txt"))) {
			int count = 0;
			Statement statement = null;

			while ((line = br.readLine()) != null) {
				if (count == 0) {
					statement = con.createStatement();
				}
				StringTokenizer token = new StringTokenizer(line);
				String personID = token.nextToken().replaceFirst("person", "");
				String wineID = token.nextToken().replaceFirst("wine", "");
				Long person = Long.parseLong(personID);
				Long wine = Long.parseLong(wineID);
				statement.addBatch("INSERT INTO WishList(pid, wid) values (" + person + " , " + wine + " )");
				count++;

				if (count == 1000) {
					statement.executeBatch();
					System.out.println("Inserted 1k Rows");
					statement.close();
					count = 0;
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Creating Database Connection
	 * 
	 **/

	public Connection getconnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(connectionURL, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return con;

	}

	/*
	 * method to insert weight for winerank and personrank table
	 * 
	 **/
	public ResultSet PrepareOutputData() {
		ResultSet resultset = null;/* final resultset */
		DoTaskMySQL obj = new DoTaskMySQL();
		try (Statement statement = con.createStatement();) {
			statement.executeUpdate(wineRankQuery);
			statement.executeUpdate(personRankQuery);
			System.out.println("weight inserted for winerank and personrank table");
			resultset = statement.executeQuery(getOutQuery);
			System.out.println("resultset fetched");
			obj.prepareFileData(resultset);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultset;

	}

	/*
	 * method to prepare output data
	 * 
	 **/
	public void prepareFileData(ResultSet resultSet) throws SQLException {
		Set<Integer> tracker = new HashSet<>();
		int bottleCnt = 0; // Total bottle count
		int curPid = -1; // Currently active Person ID
		int curCnt = 0;
		try (Writer writer = new BufferedWriter(new FileWriter(new File(outputFilename)))) {
			writer.write("                                                       \n");
			while (resultSet.next()) {
				int pid = resultSet.getInt("pid");// person id
				int wid = resultSet.getInt("wid");// bottle id
				if (curPid == pid && curCnt == 3)
					continue; // skip if the person already receives three bottles

				if (curPid != pid) {
					curPid = pid;
					curCnt = 0; // reset lastCnt
				}

				if (tracker.contains(new Integer(wid)))
					continue; // skip if the bottle is taken
				tracker.add(new Integer(wid)); // mark the bottle as "sold"

				System.out.println("Person" + pid + "\t" + "Wine" + wid + "\n");

				writer.write("Person" + pid + "\t" + "Wine" + wid + "\n");
				curCnt++;

				bottleCnt++;

			}

			System.out.println("No of bottles sold: " + bottleCnt);
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		} finally {
			con.close();
		}
		// Write the count to the first line

		try (RandomAccessFile resultFile = new RandomAccessFile(new File(outputFilename), "rws")) {
			resultFile.seek(0);
			resultFile.write(String.valueOf("Total number of wine bottles sold by Apan : " + bottleCnt).getBytes());

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * truncating all tables
	 * 
	 **/
	public void truncateQuery() {
		try (Statement statement = con.createStatement();) {
			statement.executeUpdate("truncate table WishList");
			statement.executeUpdate("truncate table WinesRank");
			statement.executeUpdate("truncate table PersonsRank");
			System.out.println("truncating all tables");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
