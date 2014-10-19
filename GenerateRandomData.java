package main.cric.schemaparser;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.GregorianCalendar;
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

	public static String randomStuff(String [] keys){
		int length = keys.length;
		int index = randBetween(0, length-1);
		return keys[index];
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String DBLocation = "database-new.cse.tamu.edu";
		String DBname = "naresh90-cricdb"; //Generally your CS username or username-text like explained above
		String DBUser = "naresh90"; //CS username
		String DBPass = "naresh"; //password setup via CSNet for the MySQL database
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null,selectstmt = null;
		
		String[] tables = {"players","teams","player_teams"}; 
		String[] attributes = {"playerId integer,playerName VARCHAR 100,dob DATE,battingStyle VARCHAR 25,bowlingStyle VARCHAR 50,playerType VARCHAR 50,placeOfBirth VARCHAR 50,height float",
				"teamId integer,teamName VARCHAR 50,teamType VARCHAR 25",
				"playerId integer,teamId integer"};
		String[] primaryforeignkeys = {"playerId|","teamId|","playerId,teamId|playerId players playerId,teamId teams teamId"};
		
		
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
			System.out.println("SQL Exception with mySql DB " + ex.getMessage());
		}
		catch (Exception e)
		{
		 System.out.println("Connection Issue during conn: " + e.getMessage());
		}
			int noofrecords = 3,startofprimarykey;
			String insertquery;
			System.out.println ("Database connection established");
			for(int i=0;i<1/*tables.length*/;i++)
			{
				startofprimarykey = 1;
				String[] constraintvalues = primaryforeignkeys[i].split("\\|");
				String[] allattributes = attributes[i].split(",");
				try
				{
					for(int j=0;j < noofrecords;j++)
					{
						//System.out.println("Inserting record..."+constraintvalues.length);
						insertquery = "insert into "+tables[i]+" values(";					
						for(int k=0;k<allattributes.length;k++)
						{
							String[] attributedetails = allattributes[k].split(" ");
							if(constraintvalues.length > 1 && constraintvalues[1].contains(attributedetails[0]))
							{
								String[] foreignkeys = constraintvalues[1].split(",");
								//if(attributedetails[0])
								for(int m=0;m<foreignkeys.length;m++)
								{
									if(foreignkeys[m].contains(attributedetails[0]))
									{
										String[] foreignkeydetails = foreignkeys[m].split(" ");
										String selectquery = "select "+foreignkeydetails[2]+" from "+foreignkeydetails[1]+";";
										selectstmt = conn.createStatement();
										ResultSet tblrs = selectstmt.executeQuery(selectquery);
										Array ans = tblrs.getArray(1);
										String[] result = (String[])ans.getArray();
										break;
									}
								}
							}
							else
							{

								//String[] primarykeys = constraintvalues[0].split(",");					
								//for(int k=0;k<allattributes.length;k++)
								//{										
								if(constraintvalues[0].contains(attributedetails[0]))
								{
									System.out.println("Primary key is "+ startofprimarykey);
									insertquery += startofprimarykey + ",";
									startofprimarykey++;
								}					
								else if(attributedetails.length == 2)
								{

									String val = randomStuff(attributedetails[1],4,15);
									if(attributedetails[1].equalsIgnoreCase("integer"))
									{
										int intval = Integer.parseInt(val);
										insertquery += intval + ",";
									}
									else if(attributedetails[1].equalsIgnoreCase("float"))
									{
										float floatval = Float.parseFloat(val);
										insertquery += floatval + ",";
									}
									else
									{
										insertquery += "'" + val + "',";
									}

								}
								else
								{
									String val = randomStuff(attributedetails[1],4,20/*Integer.parseInt(attributedetails[2])*/);
									insertquery += "'" + val + "',";
								}

								//}
							}
						}
						insertquery = insertquery.substring(0,insertquery.length()-1);
						insertquery += ");";
						System.out.println(insertquery);				
						stmt.addBatch(insertquery);
						//rs = stmt.executeQuery(insertquery);

					}	
					//stmt.executeBatch();
					conn.commit();			
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

		}
		
		/*finally{
		      //finally block used to close resources
		      try{
		         if(conn!=null)
		            conn.close();
		         if(rs != null)
		        	 rs.close();
		         if(stmt!=null)
		        	 stmt.close();
		      }catch(SQLException se){
		         se.printStackTrace();
		      }//end finally try
		   }*/

	}

