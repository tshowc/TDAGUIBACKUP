/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package my.database;
import java.util.*;
import java.sql.*;

/**
 *
 * @author chao
 */
public class QueryRawData {
    final String DATABASE = "tda"; 
	private Connection conn;
	String selectStatementPt1;
	String selectStatementPt2;
	String selectStatementPt3;
	String strQuery;
	String siteTable = "site_location";
	String tempTable = "temp_readings";
	QueryRawData(){
		this.selectStatementPt1 = "SELECT " + siteTable + ".site_name, "+ tempTable + ".temp, " + tempTable + ".month, " + tempTable + ".day, " + tempTable + ".year, "+ tempTable + ".hour ";
		this.selectStatementPt2 = "FROM " + tempTable + " JOIN " + siteTable + " ON " + siteTable + ".site_id = " + tempTable + ".site_id ";
		this.selectStatementPt3 = "WHERE (";
		this.strQuery = this.selectStatementPt1 + this.selectStatementPt2;
	}
	
	public void connectToDB(){
		 String url = "jdbc:mysql://52.36.196.165:3306/" + DATABASE;
		 String user = "tda_usr";
		 String password = "La$a1mtns!!";
		 // Load the Connector/J driver
		 try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			 // Establish connection to MySQL
				conn = DriverManager.getConnection(url, user, password);
				System.out.println("DATABASE CONNECTED");
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch(SQLException e){
			e.printStackTrace();
		}

	}
	
	public void disconnectFromDB(){
		try {
			conn.close();
			System.out.println("DATABASE DISCONNECTED");
		} catch(SQLException e){
			e.printStackTrace();
		}
	}

	public List<HashMap<String, Object>> executeQuery(String query){
		List<HashMap<String, Object>> resultList = new ArrayList<HashMap<String, Object>>();
		try {
			PreparedStatement selectStatement = conn.prepareStatement(query + ") ORDER BY " + siteTable + ".site_name," + tempTable + ".year," + tempTable + ".month," + tempTable + ".day," + tempTable + ".hour");
			System.out.println(selectStatement);
			ResultSet selectRS = selectStatement.executeQuery();
			while (selectRS.next()){
				HashMap<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put("SiteName", selectRS.getString(siteTable + ".site_name"));
				resultMap.put("Temp", selectRS.getFloat(tempTable + ".temp"));
				resultMap.put("Month", selectRS.getInt(tempTable + ".month"));
				resultMap.put("Day", selectRS.getInt(tempTable + ".day"));
				resultMap.put("Year", yearConvert(selectRS.getInt(tempTable + ".year")));
				resultMap.put("Hour", hourConvert(selectRS.getInt(tempTable + ".hour")));
				resultList.add(resultMap);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return resultList;
	}
	
	public String bySiteLocation(List<String> locations, boolean isAll){	
	 	String locationStr = locations.toString();
	 	locationStr = locationStr.replace("[", "(");
	 	locationStr = locationStr.replace("]", ")");
	 	String searchName = "site_code";
		if(!isAll && !this.strQuery.contains("WHERE")){
			this.strQuery = this.strQuery + this.selectStatementPt3 + "(" + siteTable + "." +searchName+ " IN " + locationStr + ")";
			//System.out.println(strQuery);	
		}else if(!isAll && this.strQuery.contains("WHERE")){
			this.strQuery = this.strQuery + " AND (" + siteTable + "." +searchName+ " IN " + locationStr + ")";
		}else if(isAll && this.strQuery.contains("WHERE")){
			//do nothing because by default the query is all locations
			return strQuery;			
		}
		else if (isAll && !this.strQuery.contains("WHERE")){
			this.strQuery = this.selectStatementPt1 + 
					this.selectStatementPt2;
			//System.out.println(strQuery);
		}
		return strQuery;
	}
	
	//SPLITING THE bySiteLocation into two functions? 
	//Which is better? Below or above?
	/*public String bySiteLocation(List<String> locations){
		return this.queryStringBuilder(locations.toString(), "site_code");
		
	}

	public String bySiteLocation(boolean isAll){
		if(isAll && this.strQuery.contains("WHERE")){
			//do nothing because by default the query is all locations
			return strQuery;			
		}
		else if (isAll && !this.strQuery.contains("WHERE")){
			this.strQuery = this.selectStatementPt1 + 
					this.selectStatementPt2;
			//System.out.println(strQuery);
		}
		return strQuery;
	}*/
	
	public String byDay(List<Integer> day){
		//System.out.println(day.toString());
		return this.queryBuilder(day.toString(), "day");
	}
	
	public String byHour(List<Integer> hour){
		return this.queryBuilder(hour.toString(), "hour");
	}
	
	public String byYear(List<Integer> year){
		return this.queryBuilder(year.toString(), "year");
	}
	
	public String byMonth(List<Integer> month){
		return this.queryBuilder(month.toString(), "month");
	}
	
	public String bySeasons(String season){
		List<Integer> intArray = this.getSeasonalMonths(season);
		return this.queryBuilder(intArray.toString(), "month");
	}
	
	public String bySeasons(String season, List<Integer> extraMonth){
		List<Integer> intArray = this.getSeasonalMonths(season);
		for(Object o: extraMonth){
			if(!intArray.contains(o)){
				intArray.add((Integer) o);
			}
			else{
				System.out.println(o + " is already in " + season.toUpperCase() + ".");
			}
		}
		return this.queryBuilder(intArray.toString(), "month");
	}
	
	/**
	 * Search filter spanning from July 1st FromYear to June 30th ToYear. 
	 * Reads in a List of HashMaps containing a "From" and "To" keys containing the year interval. 
	 * NOTE: This function cannot be combined with ByYear().
	 * @param LoggerYears - A List of HashMaps containing a "From" year and a "To" year.
	 * @return Returns query with logger year search params.
	 */
	public String byLoggerYear(List<HashMap<String, Integer>> LoggerYears){
		List<List<Integer>> months = this.getLoggerYearMonths();
		List<String> queries = new ArrayList<String>();
		//July 1st to June 30th, OR
		for(int i = 0 ; i < LoggerYears.size(); i++){
			Integer fromYear = LoggerYears.get(i).get("From");
			Integer toYear = LoggerYears.get(i).get("To");
			String logYearPt1 = this.loggerYearQueryPartBuilder(months.get(0).toString(), fromYear);
			String logYearPt2 = this.loggerYearQueryPartBuilder(months.get(1).toString(), toYear);
			String logYearFinal = "(" + logYearPt1 + " OR " + logYearPt2 + ")";	
			System.out.println(logYearFinal);
			queries.add(logYearFinal);
		}
		//Query Builder
		if(!queries.isEmpty()){
			if(!this.strQuery.contains("WHERE")){
				this.strQuery = this.strQuery + this.selectStatementPt3 + "(" + queries.get(0);
				for(int i = 1; i < queries.size(); i++){
					this.strQuery = this.strQuery + " OR " + queries.get(i);
				}
				this.strQuery = this.strQuery + ")";
			}else{
				this.strQuery = this.strQuery + " OR " + "(" + queries.get(0);
				for(int i=1; i < queries.size(); i++){
					this.strQuery = this.strQuery + " OR " + queries.get(i);
				}
				this.strQuery = this.strQuery + ")";
			}
		}		
		return strQuery;
	}
	
	
	/*public String byTimeInterval(List<HashMap<String, Integer>> DateList){
		List<String> queries = new ArrayList<String>();
		for(int i = 0; i < DateList.size(); i++){
			queries.add(this.timeIntervalQueryBuilder(DateList.get(i)));
			
		}
	
		return null;
	}*/
	
	public String byTimeInterval(HashMap<String, Integer> Date){
		List<Integer> months = new ArrayList<Integer>();
		List<Integer> days = new ArrayList<Integer>();
		List<Integer> years = new ArrayList<Integer>();
		List<Integer> hours = new ArrayList<Integer>();
		
	
		Integer fromMonth = Date.get("FromMonth");
		Integer toMonth = Date.get("ToMonth");
		Integer fromDay = Date.get("FromDay");
		Integer toDay = Date.get("ToDay");
		Integer fromYear = Date.get("FromYear");
		Integer toYear = Date.get("ToYear");
		Integer fromHour = Date.get("FromHour");
		Integer toHour = Date.get("ToHour");
		
		//Load up years
		int yearVal = fromYear;
		while(yearVal != toYear+1){
			years.add(yearVal);
			yearVal++;
		}
		
		//Load up months
		int monthVal = fromMonth;
	//	if(fromMonth > toMonth){
			
			while(monthVal != toMonth){
				//System.out.println("In months");
				months.add(monthVal);
				monthVal++;
				if(monthVal == 13){
					monthVal = 1;
				}
			}
			months.add(monthVal);
//		}

		//Load up days
		int dayVal = fromDay;
		while(dayVal != toDay+1){
			days.add(dayVal);
			dayVal++;
		}
		
		//Load up hours
		int hourVal = fromHour;
		while(hourVal != toHour){
			//System.out.println("In Hours");
			hours.add(hourVal);
			hourVal++;
			if(hourVal == 25){
				hourVal = 0; 
			}
		}
		hours.add(hourVal);
		
		
		//Query Building
		this.byYear(years);
		this.byMonth(months);
		this.byDay(days);
		this.byHour(hours);

		
		return strQuery;
	}
	
	private List<List<Integer>> getLoggerYearMonths(){
		List<List<Integer>> diffYearsMonth = new ArrayList<List<Integer>>();
		List<Integer> monthPt1 = new ArrayList<Integer>(); //July to December
		List<Integer> monthPt2 = new ArrayList<Integer>(); //January to June
		int pt1 = 7;
		while (pt1 != 13){
			monthPt1.add(pt1);
			pt1++;
		}
		int pt2 = 1; 
		while( pt2 != 7){
			monthPt2.add(pt2);
			pt2++;
		}
		diffYearsMonth.add(monthPt1);
	//	System.out.println(monthPt1.toString());
		diffYearsMonth.add(monthPt2);
	//	System.out.println(monthPt2.toString());		
		return diffYearsMonth;
	}
	
	
	private List<Integer> getSeasonalMonths(String season){
		ArrayList<Integer> intArray = new ArrayList<Integer>();
		switch(season.toUpperCase()){
		case "WINTER": intArray.add(12);
					   intArray.add(1);
					   intArray.add(2);
		break;
		case "SPRING": intArray.add(3);
		   			   intArray.add(4);
		   			   intArray.add(5);
		break;
		case "SUMMER": intArray.add(6);
		   			   intArray.add(7);
		   			   intArray.add(8);
		break;
		case "FALL": intArray.add(9);
		   			 intArray.add(10);
		   			 intArray.add(11);
		break;
		default: 
		}
		return intArray;
	}
	
	private Integer yearConvert(Integer year){
		String yearStr = Integer.toString(year);
		if(yearStr.length() == 2){
			yearStr = "20" + yearStr;
		}
		else if(yearStr.length() == 1){
			yearStr = "200" + yearStr;
		}
		return Integer.parseInt(yearStr);
		
	}
	
	private String hourConvert(Integer hour){
		String hourStr = Integer.toString(hour);
		hourStr =  hourStr + ":00";
		return hourStr;
	}
	
	private String loggerYearQueryPartBuilder(String part, Integer year){
		part = part.replace("[", "(");
		part = part.replace("]", ")");
		String logPart = "((" + tempTable + ".month IN " + part;
		
		logPart = logPart + ") AND (" + tempTable + ".year = " + year + "))";
		System.out.println(logPart);
		return logPart;
	}
	
	
	private String queryBuilder(String searchFilter, String searchName){
		searchFilter = searchFilter.replace("[", "(");
		searchFilter = searchFilter.replace("]", ")");
		if(!this.strQuery.contains("WHERE")){
			this.strQuery = this.strQuery + this.selectStatementPt3 + "(" + tempTable + "." +searchName+ " IN " + searchFilter + ")";

		}else{
			this.strQuery = this.strQuery + " AND (" + tempTable + "." +searchName+ " IN " + searchFilter + ")";
		}
				
		return strQuery;
		
	}
	
	
	//Goes along with the split BySiteLocation() implementation
	/*private String queryStringBuilder(String searchFilter, String searchName){
		searchFilter = searchFilter.replace("[", "(");
		searchFilter = searchFilter.replace("]", ")");
		if(!this.strQuery.contains("WHERE")){
			this.strQuery = this.strQuery + this.selectStatementPt3 + "(" + siteTable + "." +searchName+ " IN " + searchFilter + ")";
		}else{				
				this.strQuery = this.strQuery + " AND (" + siteTable + "." +searchName+ " IN " + searchFilter + ")";
		}					
			return strQuery;
	}*/
    
}
