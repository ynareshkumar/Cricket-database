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
	
	static String DBLocation = "database-new.cse.tamu.edu";
	static String DBname = "naresh90-cricdb"; //Generally your CS username or username-text like explained above
	static String DBUser = "naresh90"; //CS username
	static String DBPass = "naresh"; //password setup via CSNet for the MySQL database
	static Connection conn = null;
	ResultSet rs = null;
	static Statement stmt = null;
	static Statement selectstmt = null;

	
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
		String[] tmp = sepforeignkeys[1].split(",");
		String[] foreignKeys = new String[tmp.length];
		String[] tmp1;
		for(int i=0;i<tmp.length;i++)
		{
			tmp1 = tmp[i].split(" ");
			foreignKeys[i] = tmp1[0];
		}
		return foreignKeys;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {	
		
		int noofrecords = 3,startofprimarykey;
		String insertquery;
		String[] tables = {"players","teams","player_teams"}; 
		String[] attributes = {"playerId integer,playerName VARCHAR 100,dob DATE,battingStyle VARCHAR 25,bowlingStyle VARCHAR 50,playerType VARCHAR 50,placeOfBirth VARCHAR 50,height float",
				"teamId integer,teamName VARCHAR 50,teamType VARCHAR 25",
				"playerId integer,teamId integer"};
		String[] primaryforeignkeys = {"playerId|","teamId|","playerId,teamId|playerId players playerId,teamId teams teamId"};
		
		
		establishdatabaseconnection();
					
		System.out.println ("Database connection established");
			/*
			 * For each table in the database design.
			 */
		for(int i=0;i<1/*tables.length*/;i++)
		{
				startofprimarykey = 1;
				//Constraintvalues[0] contains all Primary keys of table and constraintvalues[1] contains all Foreign keys. 
				String[] constraintvalues = primaryforeignkeys[i].split("\\|");
				//Get each attribute details
				String[] allattributes = attributes[i].split(",");
				try
				{
					/*
					 * Generate random data for specified no of records.
					 */
					for(int j=0;j < noofrecords;j++)
					{
						insertquery = "insert into "+tables[i]+"("; 
						//Getting each column name in insert query
						
						String[] allPrimarykeys = getPrimaryKeys(primaryforeignkeys[i]);
						String[] allForeignKeys = getForeignKeys(primaryforeignkeys[i]);
						
						String[] constraintsorder = intersection(allPrimarykeys, allForeignKeys);
						for(int l=0;l<constraintsorder.length;l++)
						{
							insertquery += constraintsorder[l]+",";
						}
						
						int index = -1;
						
						for(int l=0;l<allattributes.length;l++)
						{
							String[] tmp = allattributes[l].split(" ");
							if(constraintvalues[0].contains(tmp[0]) || constraintvalues[1].contains(tmp[0]))
								continue;
							if(index == -1)
								index = l;
							insertquery += tmp[0] + ",";
						}
						insertquery = insertquery.substring(0,insertquery.length()-1);
						insertquery += ") values(";
						
						for(int s=0;s < constraintsorder.length;s++)
						{
							for(int k=0;k < allattributes.length;k++)
							{
								String[] attributedetails = allattributes[k].split(" ");
								if(constraintsorder[s].equalsIgnoreCase(attributedetails[0]))
								{
									
								}
							}
						}
						
						for(int k=index;k<allattributes.length;k++)
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

