package com.try4;


import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;



import com.opencsv.CSVReader;

public class ReadCSV 
{

	public static void main(String[] args) 
	{
		{
			 Connection conn=null;
			
			try
			{
				Class.forName("org.postgresql.Driver");
				conn=DriverManager.getConnection("jdbc:postgresql://localhost:5432/task1","postgres","Root9935");
				
				
				if(conn != null) {
					System.out.println(" database connected"); 
				}else {
					System.out.println(" connection fail"); 
				}
			
			}
			catch(Exception e)
			{
				System.out.println(e);
			
			}
			
			
			try
			{
				FileReader file = new FileReader("F:\\Task1\\COVID.csv");
				CSVReader reader=new CSVReader(file);
				String[] line;
				PreparedStatement stmt=conn.prepareStatement("Insert into twitter.covid values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				while((line=reader.readNext())!=null)
				{
					Long TweetId= Long.parseLong(line[0].replaceAll("\"", ""));
					String TweetURL=line[1];
					String TweetPostedTime= line[2];
					String TweetContent=line[3];
					String TweetType=line[4];
					String Client=line[5];
					int Retweets=Integer.parseInt(line[6]);
					int LikesReceived=Integer.parseInt(line[7]);
					String TweetLocation=line[8];
					String Lat= line[9];
					String Longitude=line[10];
					String TweetLanguage=line[11];
					Long UserId = Long.parseLong(line[12].replaceAll("\"", ""));
					String Name=line[13];
					String ScreenName=line[14];
					String UserBio=line[15];
					String VerifiedNonVerified=line[16];
					String ProfileUrl=line[17];
					String ProtectedNonProtected=line[18];
					String UserFollowing=line[19];
					String UserFollowers=line[20];
					String UserAccountCreationDate=line[21];
					
					stmt.setLong(1, TweetId);
					stmt.setString(2, TweetURL);
					stmt.setString(3, TweetPostedTime);
					stmt.setString(4, TweetContent);
					stmt.setString(5, TweetType);
					stmt.setString(6, Client);
					stmt.setInt(7, Retweets);
					stmt.setInt(8, LikesReceived);
					stmt.setString(9, TweetLocation);
					stmt.setString(10, Lat);
					stmt.setString(11, Longitude);
					stmt.setString(12, TweetLanguage);
					stmt.setLong(13, UserId);
					stmt.setString(14, Name);
					stmt.setString(15, ScreenName);
					stmt.setString(16, UserBio);
					stmt.setString(17, VerifiedNonVerified);
					stmt.setString(18, ProfileUrl);
					stmt.setString(19, ProtectedNonProtected);
					stmt.setString(20, UserFollowing);
					stmt.setString(21, UserFollowers);
					stmt.setString(22, UserAccountCreationDate);
					stmt.execute();//pb1
				}
				
			}
			catch(Exception e)
			{
				System.out.println(e);
			
			}
		}
	}

	
}
