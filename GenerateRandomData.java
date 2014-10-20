package main.cric.schemaparser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

class TableSet{
	Set<String> ts;
	TableSet(){
		ts = new TreeSet<String>();
	}
	boolean isValid(String args){
		if(ts.contains(args)){
			return false;
		}else{
			ts.add(args);
		}
		return true;
	}
}

public class GenerateData {
	
	static String DBLocation = "database-new.cse.tamu.edu";
	static String DBname = "naresh90-cricdb"; //Generally your CS username or username-text like explained above
	static String DBUser = "naresh90"; //CS username
	static String DBPass = "naresh"; //password setup via CSNet for the MySQL database
	static Connection conn = null;
	static ResultSet rs = null;
	static Statement stmt = null;
	static Statement selectstmt = null,selectstmt1 = null;
	static int startofprimarykey;
	static Writer writer = null,writer1 = null;
	
	
	public static int randBetween(int start, int end) {
		return start + (int)Math.round(Math.random() * (end - start));
	}

	public static String dateRange(){
		StringBuffer date = new StringBuffer("");
		GregorianCalendar gc = new GregorianCalendar();
		int year = randBetween(1900, 2014);
		gc.set(gc.YEAR, year);
		int dayOfYear = randBetween(1, gc.getActualMaximum(gc.DAY_OF_YEAR));
		gc.set(gc.DAY_OF_YEAR, dayOfYear);
		date.append(gc.get(gc.YEAR) + "/" + gc.get(gc.MONTH) + "/" + gc.get(gc.DAY_OF_MONTH));
		return date.toString();
	}

	public static String varCharRange(int range){
		int length = randBetween(1, range);
		final String all = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
		StringBuffer sb = new StringBuffer("");
		for(int i = 0;i<length;i++){
			sb.append(all.charAt(randBetween(0,all.length()-1)));
		}
		return sb.toString();
	}

	public static String floatRange(int start, int end){
		int left = randBetween(start, end);
		int right = randBetween(0,5);
		return left + "." + right;
	}

	public static String randomStuff(String type, int start, int end){
		if(type.equalsIgnoreCase("date")){
			return dateRange();
		}else if(type.equalsIgnoreCase("varchar")){
			return varCharRange(end);
		}else if(type.equalsIgnoreCase("float")){
			return floatRange(start,end);
		}else if(type.equalsIgnoreCase("integer")){
			Integer newInt = new Integer(randBetween(start,end));
			return newInt.toString();
		}
		return "";
	}

	public static String randomStuff(ArrayList<String> keys){
		int length = keys.size();
		int index = randBetween(0, length-1);
		return keys.get(index);
	}
	
	
	private static void establishdatabaseconnection()
	{
		try
		{
			String connectionString = "jdbc:mysql://"+DBLocation+"/"+DBname;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(connectionString, DBUser, DBPass);
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
		}
		catch(SQLException ex)
		{
			System.out.println("SQL Exception with mySql DB during connection " + ex.getMessage());
			System.exit(0);
		}
		catch (Exception e)
		{
		 System.out.println("Connection Issue during connection: " + e.getMessage());
		 System.exit(0);
		}
	}
	
	public static String[] intersection(String []pk, String []fk){
		String[] result;
		Set<String> a = new TreeSet<String>();
		for(int i=0; i<pk.length; i++){
			a.add(pk[i]);
		}
		for(int i=0; i<fk.length; i++){
			a.add(fk[i]);
		}
		result = a.toArray(new String[0]);
		return result;
	}
	
	private static String[] getPrimaryKeys(String allconstraints)
	{
		String[] sepprimarykeys = allconstraints.split("\\|");
		String[] primarykeys = sepprimarykeys[0].split(",");
		return primarykeys;
	}
	
	private static String[] getForeignKeys(String allconstraints)
	{		
		String[] sepforeignkeys = allconstraints.split("\\|");
		String[] foreignKeys = null;
		if(sepforeignkeys.length > 1)
		{
			//System.out.println("Getting all foreign keys!");
			String[] tmp = sepforeignkeys[1].split(",");
			foreignKeys = new String[tmp.length];
			String[] tmp1;
			for(int i=0;i<tmp.length;i++)
			{
				tmp1 = tmp[i].split(" ");
				foreignKeys[i] = tmp1[0];
			}
			return foreignKeys;
		}
		//System.out.println("Returning from getForeignKeys");
		return foreignKeys;
	}
	
	private static ArrayList<String> resulSettoArray(ResultSet tblrs)
	{
		ArrayList<String> result = new ArrayList<String>();
		
		try {
			while(tblrs.next())
			{
				String em = tblrs.getString(1);
				result.add(em);
			}
		} catch (SQLException e) {
			System.out.println("Error while converting from result set to array "+ e.getMessage());
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	private static String getRandomForeignKeyValue(String foreignkey,String allconstraints)
	{
		String[] sepforeignkeys = allconstraints.split("\\|");
		String[] tmp = new String[3],temporary;
		if(sepforeignkeys.length > 1 )
		{
			//System.out.println("Foreign key present!");
			temporary = sepforeignkeys[1].split(",");			
			for(int l=0;l<temporary.length;l++)
			{
				tmp = temporary[l].split(" ");
				if(temporary[l].contains(foreignkey))
						break;
			}
			//System.out.println("tmp is "+tmp[2]);
			try
			{
				selectstmt1 = conn.createStatement();
				String selectquery = "select "+ tmp[2] +" from "+tmp[1]+";";
				//System.out.println("Select query is "+selectquery);
				ResultSet tblrs = selectstmt1.executeQuery(selectquery);
				ArrayList<String> result = resulSettoArray(tblrs);
				//Array ans = tblrs.getArray(1);
				//System.out.println("Fine!!");				
				return randomStuff(result);
			}
			catch(SQLException ex)
			{
				System.out.println("SQL Exception with mySql DB during connection " + ex.getMessage());
				System.exit(0);
			}
			catch (Exception e)
			{
				System.out.println("Connection Issue during foreignkey: " + e.getMessage());
				System.exit(0);
			}
		}
		System.out.println("No value found!");
		return null;
	}
	
	private static String getUniqueRandomValueforaRecord(String[] allPrimarykeys,String[] allForeignKeys,String primaryforeignkeys,TableSet uniqueconstraintset)
	{
		Map<String,String> mapofForeignKeyValues = new LinkedHashMap<String,String>();
		
		String uniqueString;
		do
		{
			if(allForeignKeys != null)
			{
				//System.out.println("All foreign keys length");
				for(int c=0;c<allForeignKeys.length;c++)
				{
					String randomString = getRandomForeignKeyValue(allForeignKeys[c],primaryforeignkeys);
					mapofForeignKeyValues.put(allForeignKeys[c], randomString);
				}
			}
			uniqueString="";
			//System.out.println("All primary unique");
			for(int c=0;c<allPrimarykeys.length;c++)
			{
				if(mapofForeignKeyValues.containsKey(allPrimarykeys[c]))
				{
					uniqueString += mapofForeignKeyValues.get(allPrimarykeys[c]) +"-";
					mapofForeignKeyValues.remove(allPrimarykeys[c]);
				}
				else
				{
					uniqueString += startofprimarykey + "-";
					startofprimarykey++;
				}
			}

			uniqueString = uniqueString.substring(0,uniqueString.length()-1);
		}while(!uniqueconstraintset.isValid(uniqueString));
		//System.out.println("Got unique record");
		uniqueString += ";";
		for (Map.Entry<String,String> entry : mapofForeignKeyValues.entrySet()) {
			 
			  String value = entry.getValue();
			  uniqueString += value + " ";
			}		
		uniqueString = uniqueString.substring(0,uniqueString.length()-1);
		return uniqueString;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {	
		
		int noofrecords = 3;
		String insertquery,samplevalue;
		try {

			writer = new FileWriter("insertqueries.sql");	
			writer1 = new FileWriter("samplevalues.txt");

			/*String[] tables = {"players","teams","player_teams"}; 
			String[] attributes = {"playerId integer,playerName VARCHAR 100,dob DATE,battingStyle VARCHAR 25,bowlingStyle VARCHAR 50,playerType VARCHAR 50,placeOfBirth VARCHAR 50,height float",
					"teamId integer,teamName VARCHAR 50,teamType VARCHAR 25",
			"playerId integer,teamId integer"};
			String[] primaryforeignkeys = {"playerId|","teamId|","playerId,teamId|playerId players playerId,teamId teams teamId"};

			*/
			
			String[] tables = {"players","teams","player_teams","tournaments","grounds","matches","debuts","individual_scores","tournament_teams","extras"}; 
			String[] attributes = {"playerId integer,playerName VARCHAR 100,dob DATE,battingStyle VARCHAR 25,bowlingStyle VARCHAR 50,playerType VARCHAR 50,placeOfBirth VARCHAR 50,height float",
					"teamId integer,teamName VARCHAR 50,teamType VARCHAR 25",
					"playerId integer,teamId integer",
					"tournamentId integer,tournamentWinner integer,man_of_the_series integer,tournamentName VARCHAR 100,tournamentPrice integer",
					"groundId integer,groundName VARCHAR 100,capacity integer",
					"matchId integer,tournamentId integer,groundId integer,teamId1 integer,teamId2 integer,captain1 integer,captain2 integer,wkt1 integer,wkt2 integer,wonBy integer,toss integer,manOfMatch integer,matchType VARCHAR 10,dateOfMatch Date,verdict VARCHAR 5,tossDecision VARCHAR 10",
					"matchId integer,playerId integer",
					"matchId integer,playerId integer,bowler integer,fielder integer,runsScored integer,ballsFaced integer,fours integer,sixes integer,position integer,notOut integer,modeOfDismissal VARCHAR 25,overs float,maiden integer,runsGiven integer,wickets integer",
					"tournamentId integer,teamId integer",
					"matchId integer,playerId integer,inningsId integer,byes integer,legByes integer,wides integer,noBalls integer"};
			String[] primaryforeignkeys = {"playerId|",
					"teamId|",
					"playerId,teamId|playerId players playerId,teamId teams teamId",
					"tournamentId|tournamentWinner teams teamId,man_of_the_series players playerId",
					"groundId|",
					"matchId|tournamentId tournaments tournamentId,groundId grounds groundId,teamId1 teams teamId,teamId2 teams teamId,captain1 players playerId,captain2 players playerId,wkt1 players playerId,wkt2 players playerId,wonBy teams teamId,toss teams teamId,manOfMatch players playerId",
					"matchId,playerId|matchId matches matchId,playerId players playerId",
					"matchId,playerId,inningsId|matchId matches matchId,playerId players playerId,bowler players playerId,fielder players playerId",
					"tournamentId,teamId|tournamentId tournaments tournamentId,teamId teams teamId",
					"matchId,playerId,inningsId|matchId matches matchId,playerId players playerId"};
			
			establishdatabaseconnection();

			System.out.println ("Database connection established");
			/*
			 * For each table in the database design.
			 */
			for(int i=0;i<tables.length;i++)
			{
				startofprimarykey = 1;
				//Constraintvalues[0] contains all Primary keys of table and constraintvalues[1] contains all Foreign keys. 
				String[] constraintvalues = primaryforeignkeys[i].split("\\|");
				TableSet uniqueconstraintset = new TableSet();
				//Get each attribute details
				String[] allattributes = attributes[i].split(",");
				try
				{
					/*
					 * Generate random data for specified no of records.
					 */
					samplevalue = tables[i] +":\n";
					writer1.write(samplevalue);
					for(int j=0;j < noofrecords;j++)
					{
						samplevalue = "";
						insertquery = "insert into "+tables[i]+"("; 
						//Getting each column name in insert query
						//System.out.println("Before primary keys");

						String[] allPrimarykeys = getPrimaryKeys(primaryforeignkeys[i]);
						String[] allForeignKeys = getForeignKeys(primaryforeignkeys[i]);

						//System.out.println("Getting unique record");						
						String uniqueString = getUniqueRandomValueforaRecord(allPrimarykeys,allForeignKeys,primaryforeignkeys[i],uniqueconstraintset);
						//System.out.println("chumma");						
						for(int c=0;c < allPrimarykeys.length;c++)
						{
							insertquery += allPrimarykeys[c] + ",";
						}

						int index = allPrimarykeys.length;
						if(allForeignKeys != null)
						{
							for(int c=0;c<allForeignKeys.length;c++)
							{
								if(constraintvalues[0].contains(allForeignKeys[c]))
									continue;								
								index++;
								insertquery += allForeignKeys[c] + ",";								
							}
						}

						for(int c = index;c<allattributes.length;c++)
						{
							String[] temp = allattributes[c].split(" ");
							insertquery += temp[0] + ",";
						}
						insertquery = insertquery.substring(0,insertquery.length()-1);
						insertquery += ") values(";
						
						String[] sepconstraints = uniqueString.split(";");
						String[] alluniquevalues = sepconstraints[0].split("-");
						for(int c=0;c < alluniquevalues.length;c++)
						{
							insertquery += "'" + alluniquevalues[c] + "',";
							samplevalue += "'" + alluniquevalues[c] + "',"; 
						}
						
						if(sepconstraints.length > 1)
						{
							String[] allforeignkeyvalues = sepconstraints[1].split(" ");
							for(int c=0;c < allforeignkeyvalues.length;c++)
							{
								insertquery += "'" + allforeignkeyvalues[c] + "',";
								samplevalue += "'" + allforeignkeyvalues[c] + "',"; 
							}
						}
						
						if(i == 3)
						{
							System.out.println("Insert query is "+insertquery);
						}

						for(int k=index;k<allattributes.length;k++)
						{
							String[] attributedetails = allattributes[k].split(" ");							
							if(attributedetails.length == 2)
							{
								
								String val = randomStuff(attributedetails[1],4,15);								
								if(attributedetails[1].equalsIgnoreCase("integer"))
								{
									int intval = Integer.parseInt(val);
									insertquery += intval + ",";
									if(i == 3)
									{
										System.out.println("Inside all other attributes!"+val);
									}
									samplevalue += intval + ",";
								}
								else if(attributedetails[1].equalsIgnoreCase("float"))
								{
									float floatval = Float.parseFloat(val);
									insertquery += floatval + ",";
									samplevalue += floatval + ",";
								}
								else
								{
									insertquery += "'" + val + "',";
									samplevalue += "'" + val + "',";
								}

							}
							else
							{
								String val = randomStuff(attributedetails[1],4,Integer.parseInt(attributedetails[2]));
								insertquery += "'" + val + "',";
								samplevalue += "'" + val + "',";
							}

						}
						insertquery = insertquery.substring(0,insertquery.length()-1);
						samplevalue = samplevalue.substring(0,samplevalue.length()-1);
						insertquery += ");";
						System.out.println(insertquery);	
						writer.write(insertquery);
						writer.write("\n");
						writer1.write(samplevalue);
						writer1.write("\n");
						stmt.addBatch(insertquery);
						//rs = stmt.executeQuery(insertquery);

					}	
					stmt.executeBatch();
					conn.commit();		
					writer.write("\n");
				}
				catch(SQLException ex)
				{
					System.out.println("SQL Exception with mySql DB " + ex.getMessage());
				}
				catch (Exception e)
				{
					System.out.println("Connection Issue: " + e.getMessage());
				}	

			}

		} catch (IOException e) {

		System.err.println("Error writing the file : ");
		e.printStackTrace();

		} 
		finally{
			try{
		         if(conn!=null)
		            conn.close();
		         if(rs != null)
		        	 rs.close();
		         if(stmt!=null)
		        	 stmt.close();
		         if(writer != null)
		        	 writer.close();
		         if(writer1 != null)
		        	 writer1.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }
			catch (IOException e) {
				System.err.println("Error closing the file : ");
                e.printStackTrace();
			  }
//end finally try
		}
		
			
		}
				
	}

