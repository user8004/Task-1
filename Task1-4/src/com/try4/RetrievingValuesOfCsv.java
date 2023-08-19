package com.try4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Scanner;

public class RetrievingValuesOfCsv 
{

	public static void main(String[] args)
	{
		Scanner s = new Scanner(System.in);
		System.out.println("enter TweetId");
		Long tweet_id=s.nextLong();
		

		
		try
		{
			Class.forName("org.postgresql.Driver");
			Connection con=DriverManager.getConnection("jdbc:postgresql://localhost:5432/Task1","postgres","Root9935");
			
			System.out.println("tweet_id >>> "+tweet_id);
			
			
			PreparedStatement ps = con.prepareStatement("Select tweet_content from twitter.covid where tweet_id=? ");
			ps.setLong(1,tweet_id);
			
			
			ResultSet rs =ps.executeQuery();
			
			if(rs.next())
			{
				System.out.println("Tweet is ");
				System.out.println(rs.getString("tweet_content"));
			}
			else
			{
				System.out.println("wrong credential");
			}
			rs.close();
			ps.close();
			con.close();
		}
		
		catch(Exception e)
		{
			System.out.println(e);
		}
	}

}


