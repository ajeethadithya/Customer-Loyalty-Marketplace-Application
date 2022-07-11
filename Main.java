import java.util.*;
import java.sql.*;
import java.util.Scanner;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.text.SimpleDateFormat;  
// import java.util.Date;  



public class Main {

    private static String jdbcURL = "jdbc:oracle:thin:@ora.csc.ncsu.edu:1521:orcl01";
    private static final String user = "srames22";
    private static final String password = "abcd1234";
    static Connection conn = null;
    static Statement stmt = null;

    private static PreparedStatement addLoyaltyProgramInformation;
    private static PreparedStatement loginUserType;
    private static PreparedStatement insertbrand;
    private static PreparedStatement insertuser;
    private static PreparedStatement insertcustomer;
    private static PreparedStatement insertwallet;   
    private static PreparedStatement getLoyaltyProgramInformation;
    private static PreparedStatement findMinTier;
    private static PreparedStatement enrollcustomerinlp;
    private static PreparedStatement checkifcustomeraleadyinlp;

    private static PreparedStatement query1;
    private static PreparedStatement query2;
    private static PreparedStatement query3;
    private static PreparedStatement query4;
    private static PreparedStatement query5;
    private static PreparedStatement query6;
    private static PreparedStatement query7;
    private static PreparedStatement query8;

    private static PreparedStatement showBrandInfo;
    private static PreparedStatement showCustomerInfo;
    private static PreparedStatement addActivityTypeInformation;
    private static PreparedStatement addRewardTypeInformation;
    private static PreparedStatement linkbrandlp;
    private static PreparedStatement linklptier;
    private static PreparedStatement insertlptiers;

    private static PreparedStatement addRERules;   
    private static PreparedStatement addRRRules;  

    private static PreparedStatement findMaxVersionRE;
    private static PreparedStatement updateRERulesInformation;    

    private static PreparedStatement findMaxVersionRR;
    private static PreparedStatement updateRRRulesInformation;
    private static PreparedStatement showLPInfo;
    private static PreparedStatement getLPfromBID;
    private static PreparedStatement getBIDfromLP;


    private static PreparedStatement findMultiplierRAF;
    private static PreparedStatement getBIDforNum_Of_PointsRAF;
    private static PreparedStatement getbre_idforNum_Of_PointsRAF;
    private static PreparedStatement getNumber_Of_PointsRAF;
    private static PreparedStatement addLoyalty_ActivityRAF;

    private static PreparedStatement findMultiplierLAR;
    private static PreparedStatement getBIDforNum_Of_PointsLAR;
    private static PreparedStatement getbre_idforNum_Of_PointsLAR;
    private static PreparedStatement getNumber_Of_PointsLAR;
    private static PreparedStatement addLoyalty_ActivityLAR;

    private static PreparedStatement showWalletInfo;

    private static PreparedStatement showLoyaltyProgramsforCustomer; 
    private static PreparedStatement getBIDforRewardActivities;
    private static PreparedStatement getActivityCodesforRewardActivities;

    private static PreparedStatement addLinkREBrandInfo;
    private static PreparedStatement addLinkRRBrandInfo;
    private static PreparedStatement updateLinkREBrandInfo;
    private static PreparedStatement updateLinkRRBrandInfo;


    //redeem points
    private static PreparedStatement getLPCode;
    private static PreparedStatement getBID;
    private static PreparedStatement getBRRID;
    private static PreparedStatement displayRewardRedeemingRules;
    private static PreparedStatement getRewardInformation;
    private static PreparedStatement getBrandWalletInfo;
    private static PreparedStatement getBIDUsingBRRID;
    private static PreparedStatement updateWalletPoints;
    private static PreparedStatement updateNoOfInstances;
    private static PreparedStatement updateRewardActivity;


    private static PreparedStatement readActivityTypes;
    private static PreparedStatement readRewardTypes;

    private static PreparedStatement addBrandActivityTypesInformation;
    private static PreparedStatement addBrandRewardTypesInformation;

    private static PreparedStatement findMultiplierPUR;
    private static PreparedStatement getBIDforNum_Of_PointsPUR;
    private static PreparedStatement getbre_idforNum_Of_PointsPUR;
    private static PreparedStatement getNumber_Of_PointsPUR;
    private static PreparedStatement addLoyalty_ActivityPUR;

    private static PreparedStatement getPointsforRRR;
    private static PreparedStatement getWallet_IDforPUR;
    private static PreparedStatement addWallet_ActivityInfo;
    private static PreparedStatement getPoints_Earned;
    private static PreparedStatement updateWalletInfo;
    private static PreparedStatement addReward_Activity;
    private static PreparedStatement updateNoOfInstancesPUR;   
    private static PreparedStatement getBrrid;

    private static PreparedStatement getWallet_IDforActivities;

    private static PreparedStatement insertbrandwallet;
    private static PreparedStatement updateBrandWalletInfo;



    // public static String convertDate(String inputDate) {
    //     String formattedDate = "";
    //     for (int i=0; i<inputDate.size; i++) {
    //         if (inputDate.charAt(i) == '/') {
    //             formattedDate += '-';
    //         }
    //         else {
    //             formattedDate += inputDate.charAt(i);
    //         }
    //     }
    //     return formattedDate;
    // }


    public static void CreatePreparedStatements(Connection conn) {
        String query;

        try {
            query = "INSERT INTO LOYALTY_PROGRAMS" + "(LP_CODE,LP_NAME)" + " VALUES (?, ?)";
            addLoyaltyProgramInformation = conn.prepareStatement(query); 

            query = "SELECT USER_TYPE from USERS where USER_ID = ? and PASSWORD = ?";
            loginUserType = conn.prepareStatement(query);

            query = "insert into Brands (BID, NAME, ADDRESS, JOIN_DATE) values (?, ?, ?, ?)";
            insertbrand = conn.prepareStatement(query);

            query = "insert into USERS (USER_ID, password, USER_TYPE) values (?, ?, ?)";
            insertuser = conn.prepareStatement(query);

            query = "insert into CUSTOMERS (CID, NAME, PHONE_NUMBER, ADDRESS, WALLET_ID) values (?, ?, ?, ?, ?)";
            insertcustomer = conn.prepareStatement(query);

            query = "insert into WALLET (WALLET_ID, CID, POINTS_EARNED) values (?, ?, ?)";
            insertwallet = conn.prepareStatement(query);

            query = "insert into CUSTOMER_BRAND_WALLET (WALLET_ID, BID, CID, LP_POINTS_EARNED) values (?, ?, ?, ?)";
            insertbrandwallet = conn.prepareStatement(query);

            query = "SELECT * FROM LOYALTY_PROGRAMS";
            getLoyaltyProgramInformation = conn.prepareStatement(query);
        
            query = "select tier_id from (select tier_id, multiplier from LOYALTY_PROGRAM_TIERS where tier_id in (select tier_id from LINK_LOYALTY_TIERS where LP_CODE = ?)) where multiplier = ((select min(multiplier) from LOYALTY_PROGRAM_TIERS where tier_id in (select tier_id from LINK_LOYALTY_TIERS where LP_CODE = ?)))";
            findMinTier = conn.prepareStatement(query);

            query = "insert into CUSTOMER_LP_TIER (CID, LP_CODE, TIER_STATUS) values (?, ?, ?)";
            enrollcustomerinlp = conn.prepareStatement(query);

            query = "select * from CUSTOMER_LP_TIER where cid = ? and lp_code = ?";
            checkifcustomeraleadyinlp = conn.prepareStatement(query);

            query = "SELECT C.CID, C.NAME FROM CUSTOMERS C WHERE C.CID NOT IN ( SELECT CLT.CID FROM CUSTOMER_LP_TIER CLT, BRAND_LP BL WHERE CLT.LP_CODE = BL.LP_CODE AND BL.BID = 'Brand02' ) ORDER BY CID";
            query1 = conn.prepareStatement(query);  

            query = "SELECT C.CID, C.LP_CODE FROM CUSTOMER_LP_TIER C, BRAND_LP B WHERE C.LP_CODE = B.LP_CODE AND BID NOT IN (SELECT L.BID FROM LOYALTY_ACTIVITY L WHERE L.CID = C.CID) ORDER BY C.CID";
            query2 = conn.prepareStatement(query); 

            query = "SELECT DISTINCT LRB.B_RR_ID, R.REWARD_CODE, R.REWARD_NAME FROM LINK_RR_BRAND LRB, REWARD_REDEEMING_RULES RRR, REWARD_TYPES R WHERE LRB.B_RR_ID = RRR.B_RR_ID AND RRR.REWARD_CODE = R.REWARD_CODE ";
            query3 = conn.prepareStatement(query);  

            query = "SELECT BL.LP_CODE FROM BRAND_LP BL, LINK_RE_BRAND LRB, REWARD_EARNING_RULES RER WHERE BL.BID = LRB.BID AND LRB.B_RE_ID = RER.B_RE_ID AND RER.ACTIVITY_CODE = 'A03'";
            query4 = conn.prepareStatement(query); 

            query = "SELECT ACTIVITY_CODE, COUNT(*) AS INSTANCE_COUNT FROM LOYALTY_ACTIVITY WHERE BID = 'Brand01' GROUP BY ACTIVITY_CODE ORDER BY ACTIVITY_CODE";
            query5 = conn.prepareStatement(query); 

            query = "SELECT CID FROM REWARD_ACTIVITY WHERE BID = 'Brand01' GROUP BY CID HAVING COUNT(*) > 1 ORDER BY CID";
            query6 = conn.prepareStatement(query); 

            query = "SELECT BID,SUM(POINTS_REDEEMED) FROM REWARD_ACTIVITY GROUP BY BID HAVING SUM(POINTS_REDEEMED) < 500 ORDER BY BID";
            query7 = conn.prepareStatement(query); 

            query = "SELECT COUNT(*) AS NO_OF_ACTIVITIES FROM LOYALTY_ACTIVITY WHERE BID = 'Brand02' AND ACTIVITY_DATE BETWEEN TO_DATE('2021-AUG-01','YYYY-MM-DD') AND TO_DATE('2021-SEP-30','YYYY-MM-DD') GROUP BY CID HAVING CID = 'C0003' ";
            query8 = conn.prepareStatement(query); 

            query = "SELECT * FROM BRANDS where BID = ? ";
            showBrandInfo = conn.prepareStatement(query);

            query = "SELECT * FROM CUSTOMERS where CID = ? ";
            showCustomerInfo = conn.prepareStatement(query);

            query = "INSERT INTO ACTIVITY_TYPES" + "(ACTIVITY_CODE,ACTIVITY_NAME)" + " VALUES (?, ?)";
            addActivityTypeInformation = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_TYPES" + "(REWARD_CODE,REWARD_NAME)" + " VALUES (?, ?)";
            addRewardTypeInformation = conn.prepareStatement(query);

            query = "insert into BRAND_LP (BID, LP_CODE) values (?, ?)";
            linkbrandlp = conn.prepareStatement(query);

            query = "insert into LINK_LOYALTY_TIERS (LP_CODE, TIER_ID) values (?, ?)";
            linklptier = conn.prepareStatement(query);

            query = "insert into LOYALTY_PROGRAM_TIERS (TIER_ID, TIER, POINTS_REQUIRED, MULTIPLIER, RANK) values (?, ?, ?, ?, ?)";
            insertlptiers = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_EARNING_RULES" + "(B_RE_ID, BID, ACTIVITY_CODE, NO_OF_POINTS, VERSION_NO)" + " VALUES (?, ?, ?, ?, ?)";
            addRERules = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_REDEEMING_RULES" + "(B_RR_ID, BID, REWARD_CODE, NO_OF_POINTS, NO_OF_INSTANCES, VERSION_NO)" + " VALUES (?, ?, ?, ?, ?, ?)";
            addRRRules = conn.prepareStatement(query);

            query = "select max(VERSION_NO) AS max_version, ACTIVITY_CODE from REWARD_EARNING_RULES where B_RE_ID = ? and  ACTIVITY_CODE = ? group by ACTIVITY_CODE";
            findMaxVersionRE = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_EARNING_RULES" + "(B_RE_ID, BID, ACTIVITY_CODE, NO_OF_POINTS, VERSION_NO)" + " VALUES (?, ?, ?, ?, ?)";
            updateRERulesInformation = conn.prepareStatement(query);                        

            query = "select max(VERSION_NO) AS max_version, REWARD_CODE from REWARD_REDEEMING_RULES where B_RR_ID = ? and  REWARD_CODE = ? group by REWARD_CODE";
            findMaxVersionRR = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_REDEEMING_RULES" + "(B_RR_ID, BID, REWARD_CODE, NO_OF_POINTS, NO_OF_INSTANCES, VERSION_NO)" + " VALUES (?, ?, ?, ?, ?, ?)";
            updateRRRulesInformation = conn.prepareStatement(query);

            query = "SELECT * from CUSTOMER_LP_TIER where CID = ?";
            showLPInfo = conn.prepareStatement(query);

            query = "select LP_CODE from BRAND_LP where BID = ?";
            getLPfromBID = conn.prepareStatement(query);

            query = "select BID from BRAND_LP where LP_CODE = ?";
            getBIDfromLP = conn.prepareStatement(query);

            // query = "insert into USERS values ('admin', 'admin', 'admin');";
            // addAdmin = conn.prepareStatement(query);
            // addAdmin.executeQuery();

            query = "SELECT MULTIPLIER AS multiplier FROM LOYALTY_PROGRAM_TIERS L, CUSTOMER_LP_TIER C WHERE C.TIER_STATUS = L.TIER_ID AND C.CID = ? AND C.LP_CODE= ?";
            findMultiplierRAF = conn.prepareStatement(query);

            
            query = "SELECT BID AS bid FROM BRAND_LP WHERE BRAND_LP.LP_CODE = ? ";
            getBIDforNum_Of_PointsRAF = conn.prepareStatement(query);

            query = "SELECT B_RE_ID AS bre_id FROM LINK_RE_BRAND WHERE BID = ?)";
            getbre_idforNum_Of_PointsRAF = conn.prepareStatement(query);

            query = "SELECT NO_OF_POINTS AS no_of_points FROM REWARD_EARNING_RULES WHERE REWARD_EARNING_RULES.BID = ? and REWARD_EARNING_RULES.ACTIVITY_CODE = ? order by VERSION_NO DESC";
            getNumber_Of_PointsRAF = conn.prepareStatement(query);

            query = "INSERT INTO LOYALTY_ACTIVITY" + "(LOYALTY_ACTIVITY_AUTO_ID, CID, BID, ACTIVITY_CODE, POINTS_GAINED, ACTIVITY_DATE, CONTENT)" + " VALUES (?, ?, ?, ?, ?, ?, ?)";
            addLoyalty_ActivityRAF = conn.prepareStatement(query);

            query = "SELECT MULTIPLIER AS multiplier FROM LOYALTY_PROGRAM_TIERS L, CUSTOMER_LP_TIER C WHERE C.TIER_STATUS = L.TIER_ID AND C.CID = ? AND C.LP_CODE= ?";
            findMultiplierLAR = conn.prepareStatement(query);

            query = "SELECT BID AS bid FROM BRAND_LP WHERE BRAND_LP.LP_CODE = ? ";
            getBIDforNum_Of_PointsLAR = conn.prepareStatement(query);

            query = "INSERT INTO LOYALTY_ACTIVITY" + "(LOYALTY_ACTIVITY_AUTO_ID, CID, BID, ACTIVITY_CODE, POINTS_GAINED, ACTIVITY_DATE, CONTENT)" + " VALUES (?, ?, ?, ?, ?, ?, ?)";
            addLoyalty_ActivityLAR = conn.prepareStatement(query);

            query = "SELECT B_RE_ID AS bre_id FROM LINK_RE_BRAND WHERE BID = ? ";
            getbre_idforNum_Of_PointsLAR = conn.prepareStatement(query);

            query = "SELECT NO_OF_POINTS AS no_of_points FROM REWARD_EARNING_RULES WHERE REWARD_EARNING_RULES.BID = ? and REWARD_EARNING_RULES.ACTIVITY_CODE = ? order by VERSION_NO DESC";
            getNumber_Of_PointsLAR = conn.prepareStatement(query);

            query = "SELECT POINTS_EARNED FROM WALLET WHERE CID = ? ";
            showWalletInfo = conn.prepareStatement(query);

            // query = "SELECT LOYALTY_PROGRAMS.LP_CODE AS LP_CODE, LOYALTY_PROGRAMS.LP_NAME AS LP_NAME FROM CUSTOMER_LP_TIER, LOYALTY_PROGRAMS WHERE CUSTOMER_LP_TIER.LP_CODE = LOYALTY_PROGRAMS.LP_CODE and CUSTOMER_LP_TIER.CID = ?";
            query = "select LP_CODE from CUSTOMER_LP_TIER where CID = ?";
            showLoyaltyProgramsforCustomer = conn.prepareStatement(query);

            query = "SELECT BID as bid FROM BRAND_LP WHERE LP_CODE = ? ";
            getBIDforRewardActivities = conn.prepareStatement(query);

            query = "SELECT DISTINCT ACTIVITY_TYPES.ACTIVITY_CODE AS ACTIVITY_CODE, ACTIVITY_TYPES.ACTIVITY_NAME AS ACTIVITY_NAME FROM ACTIVITY_TYPES , BRAND_ACTIVITY_TYPES WHERE BRAND_ACTIVITY_TYPES.BID = ? AND BRAND_ACTIVITY_TYPES.ACTIVITY_CODE = ACTIVITY_TYPES.ACTIVITY_CODE";
            getActivityCodesforRewardActivities = conn.prepareStatement(query);            

            query = "INSERT INTO LINK_RE_BRAND" + "( B_RE_ID , BID , VERSION_NO) " + " VALUES (?, ?, ?)";
            addLinkREBrandInfo = conn.prepareStatement(query);

            query = "INSERT INTO LINK_RR_BRAND" + "( B_RR_ID , BID , VERSION_NO) " + " VALUES (?, ?, ?)";
            addLinkRRBrandInfo = conn.prepareStatement(query);

            query = "UPDATE LINK_RE_BRAND SET VERSION_NO = ? WHERE B_RE_ID = ? and BID = ? ";
            updateLinkREBrandInfo = conn.prepareStatement(query);

            query = "UPDATE LINK_RR_BRAND SET VERSION_NO = ? WHERE B_RR_ID = ? and BID = ? ";
            updateLinkRRBrandInfo = conn.prepareStatement(query);

            query = "SELECT LP_CODE FROM CUSTOMER_LP_TIER WHERE CID = ? ";
            getLPCode = conn.prepareStatement(query);

            query = "SELECT BID FROM BRAND_LP WHERE LP_CODE = ? ";
            getBID = conn.prepareStatement(query);

            query = "SELECT B_RR_ID FROM LINK_RR_BRAND WHERE BID = ? ";
            getBRRID = conn.prepareStatement(query);

            query = "SELECT B_RR_ID, BID, REWARD_CODE, NO_OF_POINTS, NO_OF_INSTANCES FROM REWARD_REDEEMING_RULES WHERE B_RR_ID = ? ";
            displayRewardRedeemingRules = conn.prepareStatement(query);

            query = "SELECT BID, REWARD_CODE, NO_OF_POINTS, NO_OF_INSTANCES FROM REWARD_REDEEMING_RULES WHERE B_RR_ID = ?";
            getRewardInformation = conn.prepareStatement(query);

            query = "SELECT LP_POINTS_EARNED FROM CUSTOMER_BRAND_WALLET WHERE CID = ? and BID = ?";
            getBrandWalletInfo = conn.prepareStatement(query);

            query = "SELECT BID FROM LINK_RR_BRAND WHERE B_RR_ID = ? ";
            getBIDUsingBRRID = conn.prepareStatement(query);

            query = "UPDATE WALLET SET POINTS_EARNED = ? WHERE CID = ? ";
            updateWalletPoints = conn.prepareStatement(query);

            query = "UPDATE REWARD_REDEEMING_RULES SET NO_OF_INSTANCES = NO_OF_INSTANCES - 1 WHERE B_RR_ID = ? ";
            updateNoOfInstances = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_ACTIVITY VALUES (null, ?, ?, ?, ?, ?) ";
            updateRewardActivity = conn.prepareStatement(query);

            query = "SELECT A.ACTIVITY_CODE, A.ACTIVITY_NAME FROM ACTIVITY_TYPES A";
            readActivityTypes = conn.prepareStatement(query); 

            query = "SELECT A.REWARD_CODE, A.REWARD_NAME FROM REWARD_TYPES A";
            readRewardTypes = conn.prepareStatement(query);

            query = "INSERT INTO BRAND_ACTIVITY_TYPES values (?, ?)";
            addBrandActivityTypesInformation = conn.prepareStatement(query);

            query = "INSERT INTO BRAND_REWARD_TYPES values (?, ?)";
            addBrandRewardTypesInformation = conn.prepareStatement(query);

            query = "SELECT MULTIPLIER AS multiplier FROM LOYALTY_PROGRAM_TIERS L, CUSTOMER_LP_TIER C WHERE C.TIER_STATUS = L.TIER_ID AND C.CID = ? AND C.LP_CODE= ?";
            findMultiplierPUR = conn.prepareStatement(query);

            query = "SELECT BID AS bid FROM BRAND_LP WHERE BRAND_LP.LP_CODE = ? ";
            getBIDforNum_Of_PointsPUR = conn.prepareStatement(query);

            query = "SELECT B_RE_ID AS bre_id FROM LINK_RE_BRAND WHERE BID = ?";
            getbre_idforNum_Of_PointsPUR = conn.prepareStatement(query);

            query = "SELECT NO_OF_POINTS AS no_of_points FROM REWARD_EARNING_RULES WHERE REWARD_EARNING_RULES.BID = ? and REWARD_EARNING_RULES.ACTIVITY_CODE = ? order by VERSION_NO DESC";
            getNumber_Of_PointsPUR = conn.prepareStatement(query);

            query = "INSERT INTO LOYALTY_ACTIVITY" + "(LOYALTY_ACTIVITY_AUTO_ID, CID, BID, ACTIVITY_CODE, POINTS_GAINED, ACTIVITY_DATE, CONTENT)" + " VALUES (?, ?, ?, ?, ?, ?, ?)";
            addLoyalty_ActivityPUR = conn.prepareStatement(query);

            query = "SELECT WALLET_ID AS wallet_id FROM WALLET WHERE WALLET.CID = ?";
            getWallet_IDforPUR = conn.prepareStatement(query);

            query = "INSERT INTO WALLET_ACTIVITY" + "(WALLET_ACTIVITY_AUTO_ID, WALLET_ID, WALLET_ACTIVITY_DATE, BID, ACTIVITY_CODE, POINTS_EARNED_FOR_ACTIVITY)" + " VALUES (?, ?, ?, ?, ?, ?)";
            addWallet_ActivityInfo = conn.prepareStatement(query);

            query = "SELECT POINTS_EARNED AS pts FROM WALLET WHERE WALLET_ID = ? AND CID = ?";
            getPoints_Earned = conn.prepareStatement(query);            

            query = "UPDATE WALLET SET POINTS_EARNED = (POINTS_EARNED + ?) WHERE WALLET_ID = ? and CID = ? ";         
            updateWalletInfo = conn.prepareStatement(query); 

            // query = "UPDATE WALLET SET POINTS_EARNED = ? WHERE WALLET_ID = ? and CID = ? ";         
            // updateWalletInfo = conn.prepareStatement(query);

            query = "UPDATE CUSTOMER_BRAND_WALLET SET LP_POINTS_EARNED = (LP_POINTS_EARNED + ?) WHERE CID = ? and BID = ?";         
            updateBrandWalletInfo = conn.prepareStatement(query);

            // query = "SELECT B_RR_ID FROM LINK_RR_BRAND WHERE BID = ?";
            // getBrrid = conn.prepareStatement(query);

            query = "SELECT NO_OF_POINTS AS points FROM REWARD_REDEEMING_RULES WHERE BID = ? and REWARD_CODE = ? order by VERSION_NO DESC" ;
            getPointsforRRR = conn.prepareStatement(query);

            query = "INSERT INTO REWARD_ACTIVITY" + "(REWARD_ACTIVITY_AUTO_ID, CID, BID, REWARD_CODE, POINTS_REDEEMED, REDEEM_DATE)" + " VALUES (?, ?, ?, ?, ?, ?)";
            addReward_Activity = conn.prepareStatement(query);                       

            query = "UPDATE REWARD_REDEEMING_RULES SET NO_OF_INSTANCES = NO_OF_INSTANCES - 1 WHERE B_RR_ID = ? ";         
            updateNoOfInstancesPUR = conn.prepareStatement(query);

            query = "SELECT WALLET_ID AS wallet_id FROM WALLET WHERE WALLET.CID = ?";
            getWallet_IDforActivities = conn.prepareStatement(query);

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //home
    static void home() {

        System.out.println("\n---------------------------------");
        System.out.println("Home");
        System.out.println("---------------------------------\n");
        System.out.println("\nEnter your choice:");
        System.out.println("1.Login\n2.Sign Up\n3.Show Queries\n4.Exit");
		System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        System.out.println("Your choice:"+choice);
        
        switch(choice) {
            case 1:
                login();
                break;
            case 2:
                signUp();
                break;
            case 3:
                showQueries();
                break;
            case 4:
                break;
            default:
                System.out.println("\nWrong choice. Try again.");
                home();
                break;
        }
    }
    
    //1.login
    static void login() {	
        System.out.println("\n---------------------------------");
        System.out.println("Login");
        System.out.println("---------------------------------\n");
		Scanner sc = new Scanner(System.in);
        System.out.println("\nEnter Username:");
    	String username = sc.nextLine();
    	System.out.println("\nEnter Password:");
    	String password = sc.nextLine();
        System.out.println("1.Sign In\n2.Go Back");
        System.out.println("\nEnter your choice:");
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1: 
                signIn(username, password);
                break;
            case 2:
                home();
                break;
            default:
                System.out.println("\nWrong choice. Try again.");
                login();
                break;
        }
    }
// Validate with database and get the user type.
// Print login incorrect for invalid credentials.


    //1.1.sign in
    static void signIn(String username, String password) {

        System.out.println("\n---------------------------------");
        System.out.println("Signing in...");
        System.out.println("---------------------------------\n");

        String usertype = "";

        try
        {
            loginUserType.setString(1, username);
            loginUserType.setString(2, password);
            ResultSet res = loginUserType.executeQuery();
            // res.beforeFirst();
            if (res.next()) {
                usertype = res.getString("USER_TYPE");
            }
            
        }
        catch (SQLException e)
        {
            // do something appropriate with the exception, *at least*:
            e.printStackTrace();
        }
        
        switch(usertype) {
            case "admin":
                adminLanding();
                break;
            case "brand":
                brandLanding(username);
                break;
            case "customer":
                customerLanding(username);
                break;
            default:
                System.out.println("\nInvalid credentials. Enter again.");
                login();
                break;
        }
    }

    //2.sign up
    static void signUp() {

        System.out.println("\n---------------------------------");
        System.out.println("Sign up");
        System.out.println("---------------------------------\n");

        System.out.println("1.Brand Sign-Up\n2.Customer Sign-Up\n3.Go Back");
		System.out.println("\nEnter your choice:");
		Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        switch(choice) {
            case 1:
                brandSignUp();
                break;
            case 2:
                customerSignUp();
                break;
            case 3:
                home();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                signUp();
                break;
                
        }
    }
    
    //2.1.brand sign-up
    static void brandSignUp() {

        System.out.println("\n---------------------------------");
        System.out.println("Brand Sign Up");
        System.out.println("---------------------------------\n");
        
        Scanner sc = new Scanner(System.in);
        
        System.out.println("\nEnter Brand ID:");
        String bid = sc.nextLine();
        System.out.println("\nEnter Brand Name:");
        String name = sc.nextLine();
        System.out.println("\nEnter Brand Address:");
        String address = sc.nextLine();
        System.out.println("\nEnter Brand Join date:");
        String join_date = sc.nextLine();
        // System.out.println("\nEnter Brand LP Code:");
        // String lp_code = sc.nextLine();

        // java.util.Date today = new java.util.Date();
        // java.sql.Date sqldate = new java.sql.Date(today.getTime());

        java.sql.Date sqldate = java.sql.Date.valueOf(join_date);

        System.out.println("1.Sign-Up\n2.Go Back");
		System.out.println("\nEnter your choice:");
		
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save details to database in BRANDS table 
                try
                {
                    // call methods that might throw SQLException
                    insertbrand.setString(1, bid);
                    insertbrand.setString(2, name);
                    insertbrand.setString(3, address);
                    insertbrand.setDate(4, sqldate);
                    insertbrand.executeQuery();

                    insertuser.setString(1, bid);
                    insertuser.setString(2, bid);
                    insertuser.setString(3, "brand");
                    insertbrand.executeQuery();

                }
                catch (SQLException e)
                {
                    // do something appropriate with the exception, *at least*:
                    e.printStackTrace();
                }

                System.out.println("Brand added!");
                login();
                break;
            case 2:
                signUp();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                brandSignUp();
                break;
        }
    }
	
    
    //2.2.customer sign-up
    static void customerSignUp() {

        System.out.println("\n---------------------------------");
        System.out.println("Customer Sign Up");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);

        System.out.println("\nEnter Customer ID:");
        String cid = sc.nextLine();
        System.out.println("\nEnter Customer Name:");
        String name = sc.nextLine();
        System.out.println("\nEnter Customer Phone Number:");
        String phone_number_string = sc.nextLine();
        long phone_number=Long.parseLong(phone_number_string);  
        System.out.println("\nEnter Customer Address:");
        String address = sc.nextLine();
        System.out.println("\nEnter Customer Wallet ID:");
        String wallet_id = sc.nextLine();

        System.out.println("1.Sign-Up\n2.Go Back");
		System.out.println("\nEnter your choice:");
		
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save details to database in CUSTOMERS table 

                try
                {
                    // call methods that might throw SQLException
                    
                    insertcustomer.setString(1, cid);
                    insertcustomer.setString(2, name);
                    insertcustomer.setLong(3, phone_number);
                    insertcustomer.setString(4, address);
                    insertcustomer.setString(5, wallet_id);
                    insertcustomer.executeQuery();

                    insertwallet.setString(1, wallet_id);
                    insertwallet.setString(2, cid);
                    insertwallet.setInt(3, 0);
                    insertwallet.executeQuery();

                    insertuser.setString(1, cid);
                    insertuser.setString(2, cid);
                    insertuser.setString(3, "customer");
                    insertbrand.executeQuery();

                }
                catch (SQLException e)
                {
                    // do something appropriate with the exception, *at least*:
                    e.printStackTrace();
                }

                System.out.println("Customer added!");
                login();
                break;
            case 2:
                signUp();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                customerSignUp();
                break;
        }
    }


    static void printQuery1() {
        try {
            ResultSet result = query1.executeQuery();
            // result.beforeFirst();
            System.out.println("CID\tNAME\n");
            while(result.next()) {
                System.out.print(result.getString("CID") + "\t" + result.getString("NAME") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery2() {
        try {
            ResultSet result = query2.executeQuery();
            // result.beforeFirst();
            System.out.println("CID\tLP_CODE\n");
            while(result.next()) {
                System.out.print(result.getString("CID") + "\t" + result.getString("LP_CODE") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery3() {
        try {
            ResultSet result = query3.executeQuery();
            // result.beforeFirst();
            System.out.println("B_RR_ID\n");
            while(result.next()) {
                System.out.print(result.getString("B_RR_ID") + "\t" + result.getString("REWARD_CODE") + "\t" + result.getString("REWARD_NAME") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }



    static void printQuery4() {
        try {
            ResultSet result = query4.executeQuery();
            // result.beforeFirst();
            System.out.println("LP_CODE\n");
            while(result.next()) {
                System.out.print(result.getString("LP_CODE") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery5() {
        try {
            ResultSet result = query5.executeQuery();
            // result.beforeFirst();
            System.out.println("ACTIVITY_CODE\tINSTANCE COUNT\n");
            while(result.next()) {
                System.out.print(result.getString("ACTIVITY_CODE") + "\t" + result.getString("INSTANCE_COUNT") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery6() {
        try {
            ResultSet result = query6.executeQuery();
            // result.beforeFirst();
            System.out.println("CID\n");
            while(result.next()) {
                System.out.print(result.getString("CID") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery7() {
        try {
            ResultSet result = query7.executeQuery();
            // result.beforeFirst();
            System.out.println("BID\tSUM OF POINTS REDEEMED\n");
            while(result.next()) {
                System.out.print(result.getString("BID") + "\t" + result.getString("SUM(POINTS_REDEEMED)") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }


    static void printQuery8() {
        try {
            ResultSet result = query8.executeQuery();
            // result.beforeFirst();
            System.out.println("COUNT\n");
            while(result.next()) {
                System.out.print(result.getString("NO_OF_ACTIVITIES") + "\n");
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
            showQueries();
    }

     //3.show queries
    static void showQueries() {
        
        System.out.println("\n---------------------------------");
        System.out.println("Show Queries");
        System.out.println("---------------------------------\n");
        System.out.println("\nEnter your choice:");
        System.out.println("1.List all customers that are not part of Brand02’s program");
        System.out.println("2.List customers that have joined a loyalty program but have not participated in any activity in that program (list the customerid and the loyalty program id)");
        System.out.println("3.List the rewards that are part of Brand01 loyalty program");
        System.out.println("4.List all the loyalty programs that include “refer a friend” as an activity in at least one of their reward rules");
        System.out.println("5.For Brand01, list for each activity type in their loyalty program, the number instances that have occurred");
        System.out.println("6.List customers of Brand01 that have redeemed at least twice");
        System.out.println("7.All brands where total number of points redeemed overall is less than 500 points");
        System.out.println("8.For Customer C0003, and Brand02, number of activities they have done in the period of 08/1/2021 and 9/30/2021");
        System.out.println("0.Home");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        System.out.println("Your choice:"+choice);

        switch(choice) {
            case 1:
                printQuery1();
                break;
            case 2:
                printQuery2();
                break;
            case 3:
                printQuery3();
                break;
            case 4:
                printQuery4();
                break;
            case 5:
                printQuery5();
                break;
            case 6:
                printQuery6();
                break;
            case 7:
                printQuery7();
                break;
            case 8:
                printQuery8();
                break;
            case 0:
                home();
                break;

            default:
                System.out.println("\nWrong choice. Try again.");
                showQueries();
                break;
        }
    }

    
    //admin landing
    static void adminLanding() {

        System.out.println("\n---------------------------------");
        System.out.println("Admin Landing");
        System.out.println("---------------------------------\n");
        
        System.out.println("1.Add Brand\n2.Add Customer\n3.Show Brand's Info\n4.Show Customer's Info\n5.Add Activity Type\n6.Add Reward Type\n7.Log out");
		System.out.println("\nEnter your choice:");
		Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                addBrand();
                break;
            case 2:
                addCustomer();
                break;
            case 3:
                brandInfo();
                break;
            case 4:
                customerInfo();
                break;
            case 5:
                addActivityType();
                break;
            case 6:
                addRewardType();
                break;
            case 7:
                // logout
                home();
            default:
                System.out.println("\nWrong choice. Try again");
                adminLanding();
                break;
                
        }
        
    }
    
    //1.add brand
    static void addBrand() {

        System.out.println("\n---------------------------------");
        System.out.println("Add Brand");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        
        System.out.println("\nEnter Brand ID:");
        String bid = sc.nextLine();
        System.out.println("\nEnter Brand Name:");
        String name = sc.nextLine();
        System.out.println("\nEnter Brand Address:");
        String address = sc.nextLine();
        System.out.println("\nEnter Brand Join date:");
        String join_date = sc.nextLine();
        // System.out.println("\nEnter Brand LP Code:");
        // String lp_code = sc.nextLine();

        // try {
        //     java.util.Date utilDate=new SimpleDateFormat("MM/dd/yyyy").parse(join_date);
        //     java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        // }
        // catch (SQLException e) 
        // {
        //     e.printStackTrace();
        // }
    
        // java.sql.Date date=java.sql.Date.valueOf(join_date);

        // java.util.Date today = new java.util.Date();
        // java.sql.Date sqldate = new java.sql.Date(today.getTime());

        Date sqldate = Date.valueOf(join_date);        

        System.out.println("1.Add brand\n2.Go Back");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save details to database in BRANDS table 
                try
                {
                    // call methods that might throw SQLException
                    insertbrand.setString(1, bid);
                    insertbrand.setString(2, name);
                    insertbrand.setString(3, address);
                    insertbrand.setDate(4, sqldate);
                    insertbrand.executeQuery();

                    insertuser.setString(1, bid);
                    insertuser.setString(2, bid);
                    insertuser.setString(3, "brand");
                    insertuser.executeQuery();

                }
                catch (SQLException e)
                {
                    // do something appropriate with the exception, *at least*:
                    e.printStackTrace();
                }

                System.out.println("Brand added!");
                adminLanding();
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addBrand();
                break;
        }
    }

    //2.add customer
    static void addCustomer() {

        System.out.println("\n---------------------------------");
        System.out.println("Add Customer");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);

        System.out.println("\nEnter Customer ID:");
        String cid = sc.nextLine();
        System.out.println("\nEnter Customer Name:");
        String name = sc.nextLine();
        System.out.println("\nEnter Customer Phone Number:");
        String phone_number_string = sc.nextLine();
        long phone_number=Long.parseLong(phone_number_string);  
        System.out.println("\nEnter Customer Address:");
        String address = sc.nextLine();
        System.out.println("\nEnter Customer Wallet ID:");
        String wallet_id = sc.nextLine();

        System.out.println("1.Add Customer\n2.Go Back");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:

                try
                {
                    // call methods that might throw SQLException

                    insertcustomer.setString(1, cid);
                    insertcustomer.setString(2, name);
                    insertcustomer.setLong(3, phone_number);
                    insertcustomer.setString(4, address);
                    insertcustomer.setString(5, wallet_id);
                    insertcustomer.executeQuery();


                    insertwallet.setString(1, wallet_id);
                    insertwallet.setString(2, cid);
                    insertwallet.setInt(3, 0);
                    insertwallet.executeQuery();

                    insertuser.setString(1, cid);
                    insertuser.setString(2, cid);
                    insertuser.setString(3, "customer");
                    insertuser.executeQuery();

                }
                catch (SQLException e)
                {
                    // do something appropriate with the exception, *at least*:
                    e.printStackTrace();
                }

                System.out.println("Customer added!");
                adminLanding();
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addCustomer();
                break;
        }
        
    }
     

    //3.brand info
    static void brandInfo() {

        System.out.println("\n---------------------------------");
        System.out.println("Show Brand Info");
        System.out.println("---------------------------------\n");



        Scanner sc = new Scanner(System.in);
        System.out.println("\nEnter Brand ID:");
        String bid = sc.nextLine();

        System.out.println("1.Show Brand's Info\n2.Go Back");
		System.out.println("\nEnter your choice:");
		
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Select * from BRANDS where bid = bid
                // if not found, show error
                try {
                    showBrandInfo.setString(1, bid);
                    ResultSet res = showBrandInfo.executeQuery();
                    
                    if (!res.isBeforeFirst()) 
                    {    
                        System.out.println("Brand not found"); 
                    }
                    // res.beforeFirst();
                    while (res.next()) {
                        System.out.print(res.getString("BID") + "\t" + res.getString("NAME") + "\t" + res.getString("ADDRESS") + "\t" + res.getString("JOIN_DATE") + "\n");
                    }

                } 
                catch (SQLException e) 
                {
                    e.printStackTrace();
                }

                brandInfo();
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                brandInfo();
                break;
        }
        
    }

    
    //4.customer info
    static void customerInfo() {

        System.out.println("\n---------------------------------");
        System.out.println("Show Customer Info");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("\nEnter Customer ID:");
        String cid = sc.nextLine();

        System.out.println("1.Show Customer's Info\n2.Go Back");
		System.out.println("\nEnter your choice:");
		
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Select * from CUSTOMERS where cid = cid
                // if not found, show error
                try {

                    showCustomerInfo.setString(1, cid);
                    ResultSet res = showCustomerInfo.executeQuery();
                    if (!res.isBeforeFirst()) 
                    {    
                    System.out.println("Customer not found."); 
                    }
                    // res.beforeFirst();
                    while (res.next()) {
                        System.out.print(res.getString("CID") + "\t" + res.getString("NAME") + "\t" + res.getString("PHONE_NUMBER") + "\t" + res.getString("ADDRESS") + "\t" + res.getString("WALLET_ID") + "\n");
                    }

                } catch (SQLException e) 
                {
                    e.printStackTrace();
                }

                customerInfo();
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                customerInfo();
                break;
        }
        
    }
// Ask admin to enter customers user id.
// display related customer info and show error message if necessary.

    
    //5.add activity type 
    static void addActivityType() {

        System.out.println("\n---------------------------------");
        System.out.println("Add Activity Type");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("\nEnter Activity Name:");
        String activity_name = sc.nextLine();

        System.out.println("\nEnter Activity Code:");
        String activity_code = sc.nextLine();

        System.out.println("1.Add Activity Type\n2.Go Back");
		System.out.println("\nEnter your choice:");
		
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save details to database in ACTIVITY_TYPES table 

                try {
                    addActivityTypeInformation.setString(1, activity_code);
                    addActivityTypeInformation.setString(2, activity_name);
                    addActivityTypeInformation.executeQuery();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }

                System.out.println("Activity type added!");
                System.out.println("Do you want to add another activity type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");
                
                int choice2 = sc.nextInt();  
                sc.nextLine();
                switch(choice2) {
                    case 1:
                        addActivityType();
                        break;
                    case 2:
                        adminLanding();
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        addActivityType();
                        break;
                }
                
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addActivityType();
                break;
        }
        
    }
    
    //6.add reward type
    static void addRewardType() {

        System.out.println("\n---------------------------------");
        System.out.println("Add Reward Type");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("\nEnter Reward Name:");
        String reward_name = sc.nextLine();

        System.out.println("\nEnter Reward Code:");
        String reward_code = sc.nextLine();

        System.out.println("1.Add Reward Type\n2.Go Back");
		System.out.println("\nEnter your choice:");

        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save details to database in REWARD_TYPES table 
                try {
                    addRewardTypeInformation.setString(1, reward_code);
                    addRewardTypeInformation.setString(2, reward_name);
                    addRewardTypeInformation.executeQuery();
                } 
                catch(SQLException e)
                {
                    e.printStackTrace();
                }
                System.out.println("Reward type added!");
                System.out.println("Do you want to add another reward type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");
                
                int choice2 = sc.nextInt();  
                sc.nextLine();
                switch(choice2) {
                    case 1:
                        addRewardType();
                        break;
                    case 2:
                        adminLanding();
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        addRewardType();
                        break;
                }
                
                break;
            case 2:
                adminLanding();
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addRewardType();
                break;
        }
        
    }

    
    //brand landing 
    static void brandLanding(String bid) {

        System.out.println("\n---------------------------------");
        System.out.println("Brand Landing");
        System.out.println("---------------------------------\n");
        
        System.out.println("1.Add Loyalty Program\n2.Add RE Rules\n3.Update RE Rules\n4.Add RR Rules\n5.Update RR Rulese\n6.Validate Loyalty Program\n7.Log out");
		System.out.println("\nEnter your choice:");
		Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();

        String lp_code = "";

        if (choice != 1) {
            try
            {
                getLPfromBID.setString(1, bid);
                ResultSet res = getLPfromBID.executeQuery();
                if (!res.isBeforeFirst()) 
                {    
                    System.out.println("Please setup LP for the brand first");
                    addLoyaltyProgram(bid); 
                }

                // res.beforeFirst();
                if (res.next()) {
                    lp_code = res.getString("LP_CODE");
                }
                
            }
            catch (SQLException e)
            {
                // do something appropriate with the exception, *at least*:
                e.printStackTrace();
            }
        }
        
        switch(choice) {
            case 1:
                addLoyaltyProgram(bid);
                break;
            case 2:
                addRERules(bid, lp_code);
                break;
            case 3:
                updateRERules(bid, lp_code);
                break;
            case 4:
                addRRRules(bid, lp_code);
                break;
            case 5:
                updateRRRules(bid, lp_code);
                break;
            case 6:
                validateLoyaltyProgram(bid, lp_code);
                break;
            case 7:
                home();
            default:
                System.out.println("\nWrong choice. Try again");
                brandLanding(bid);
                break;
                
        }
        
    }

    //1.add loyalty program
    static void addLoyaltyProgram(String bid) {

        System.out.println("\n---------------------------------");
        System.out.println("Add Loyalty Program");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("Enter Loyalty Program Code : ");
        String lp_code = sc.nextLine();
        System.out.println("Enter Loyalty Program Name : ");
        String lp_name = sc.nextLine();

        // Insert lp_code and lp_name to LOYALTY_PROGRAMS database
        // create a link between LOYALTY_PROGRAMS and BRAND_LP using BID and LP_CODE

        System.out.println("Do you want to add this loyalty program?\n1.Yes\n2.No, Go back.\n");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();

        if (choice != 1) {
            brandLanding(bid);
        }

        try
        {
            // call methods that might throw SQLException
            addLoyaltyProgramInformation.setString(1, lp_code);
            addLoyaltyProgramInformation.setString(2, lp_name);
            addLoyaltyProgramInformation.executeQuery();

            linkbrandlp.setString(1, bid);
            linkbrandlp.setString(2, lp_code);
            linkbrandlp.executeQuery();

        }
        catch (SQLException e)
        {
            // do something appropriate with the exception, *at least*:
            e.printStackTrace();
        }

            
        
        System.out.println("1.Regular\n2.Tier\n3.Go Back");
    	System.out.println("\nEnter your choice:");
	    
        choice = sc.nextInt();
        sc.nextLine();
    
        switch(choice) {
            case 1:
                regular(bid, lp_code);
                break;
            case 2:
                tier(bid, lp_code);
                break;
            case 3:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addLoyaltyProgram(bid);
                break;
        }
    }
        
    //regular loyalty program
    static void regular(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Regular Loyalty program");
        System.out.println("---------------------------------\n");

        System.out.println("1.Activity Types\n2.Reward Types\n3.Go Back");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
            
        switch(choice) {
            case 1:
                activityTypesRegular(bid, lp_code);
                break;
            case 2:
                rewardTypesRegular(bid, lp_code);
                break;
            case 3:
                addLoyaltyProgram(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                regular(bid, lp_code);
                break;
        }
    }
    
//activity types regular
    static void activityTypesRegular(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Activity Types Regular");
        System.out.println("---------------------------------\n");


        try {

            Scanner sc = new Scanner(System.in);
            // readActivityTypes.setString(1,bid);
            ResultSet result = readActivityTypes.executeQuery();
            // result.beforeFirst();
        
            while(result.next()) {
                System.out.println(result.getString("ACTIVITY_CODE") + ": " + result.getString("ACTIVITY_NAME"));
            }

            System.out.println("0: Go Back");
            System.out.println("Enter the Activity Type Code:");
            String choice = sc.nextLine();

            if (choice=="0") {
                regular(bid, lp_code);
            }

            else {
                addBrandActivityTypesInformation.setString(1, bid);
                addBrandActivityTypesInformation.setString(2, choice);
                addBrandActivityTypesInformation.executeQuery();

                System.out.println("Do you want to add another Activity type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt(); 
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        activityTypesRegular(bid, lp_code);
                        break;
                    case 2:
                        regular(bid, lp_code);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        regular(bid, lp_code);
                        break;
                }
            }


        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
    }
    
    static void rewardTypesRegular(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Reward Types Regular");
        System.out.println("---------------------------------\n");

        try {

            Scanner sc = new Scanner(System.in);
            // readRewardTypes.setString(1,bid);
            ResultSet result = readRewardTypes.executeQuery();
            // result.beforeFirst();
        
            while(result.next()) {
                System.out.println(result.getString("REWARD_CODE") + ": " + result.getString("REWARD_NAME"));
            }

            System.out.println("0: Go Back");
            System.out.println("Enter the Reward Type Code:");
            String choice = sc.nextLine();

            // String choice = sc.nextLine();

            if (choice=="0") {
                regular(bid, lp_code);
            }
            else {
                addBrandRewardTypesInformation.setString(1, bid);
                addBrandRewardTypesInformation.setString(2, choice);
                addBrandRewardTypesInformation.executeQuery();

                System.out.println("Do you want to add another Reward type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt();  
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        rewardTypesRegular(bid, lp_code);
                        break;
                    case 2:
                        regular(bid, lp_code);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        regular(bid, lp_code);
                        break;
                }
            }

            

        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }
    }
// get quantity from user.    
    
    
    //tier loyalty program
    static void tier(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Tiered Loyalty Program");
        System.out.println("---------------------------------\n");

        System.out.println("1.Tier Set Up\n2.Activity Types\n3.Reward Types\n4.Go Back");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
    
        switch(choice) {
            case 1:
                tierSetUp(bid, lp_code);
                break;
            case 2:
                activityTypesTier(bid, lp_code);
                break;
            case 3:
                rewardTypesTier(bid, lp_code);
                break;
            case 4:
                addLoyaltyProgram(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                tier(bid, lp_code);
                break;
        }
    }
    
    //tier set up
    static void tierSetUp(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Tier Setup");
        System.out.println("---------------------------------\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("Number of tiers");
        int n_tiers = sc.nextInt();
        sc.nextLine();
        String tier = "";
        String tier_id = "";
        int points_required = 0;
        int multiplier = 1;
        

        System.out.println("1.Set Up\n2.Go Back");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();
    
        switch(choice) {
            case 1:
                // Add all the details to LOYALTY_PROGRAM_TIERS table
                for (int i=0; i<n_tiers; i++) {
                    System.out.println("Unique tier ID " + i + " : ");
                    tier_id = sc.nextLine();

                    System.out.println("Name of tier " + i + " : ");
                    tier = sc.nextLine();
                    
                    System.out.println("Points required for tier " + i + " : ");
                    points_required = sc.nextInt();
                    sc.nextLine();

                    System.out.println("Multiplier for tier " + i + " : ");
                    multiplier = sc.nextInt();
                    sc.nextLine();
                    try
                    {
                        // call methods that might throw SQLException
                        insertlptiers.setString(1, tier_id);
                        insertlptiers.setString(2, tier);
                        insertlptiers.setInt(3, points_required);
                        insertlptiers.setInt(4, multiplier);
                        insertlptiers.setInt(5, i);
                        insertlptiers.executeQuery();

                        linklptier.setString(1, lp_code);
                        linklptier.setString(2, tier_id);
                        linklptier.executeQuery();

                        System.out.println("Added loyalty program tiers.");

                    }
                    catch (SQLException e)
                    {
                        // do something appropriate with the exception, *at least*:
                        e.printStackTrace();
                    }
                }
                tier(bid, lp_code);
                break;
            case 2:
                tier(bid, lp_code);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                tierSetUp(bid, lp_code);
                break;
        }
    }
// get number of tiers from user
// get name of tiers in increasing order of precedence from user.
// points required (lower bound).    
// multipliers per tier.
    
    //activity types tier
    static void activityTypesTier(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Activity Types Tier");
        System.out.println("---------------------------------\n");


        try {

            Scanner sc = new Scanner(System.in);
            // readActivityTypes.setString(1,bid);
            ResultSet result = readActivityTypes.executeQuery();
            // result.beforeFirst();
        
            while(result.next()) {
                System.out.println(result.getString("ACTIVITY_CODE") + ": " + result.getString("ACTIVITY_NAME"));
            }

            System.out.println("0: Go Back");
            System.out.println("Enter the Activity Type Code:");
            String choice = sc.nextLine();

            if (choice=="0") {
                tier(bid, lp_code);
            }
            else {
                addBrandActivityTypesInformation.setString(1, bid);
                addBrandActivityTypesInformation.setString(2, choice);
                addBrandActivityTypesInformation.executeQuery();

                System.out.println("Do you want to add another Activity type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt();  
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        activityTypesTier(bid, lp_code);
                        break;
                    case 2:
                        tier(bid, lp_code);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        tier(bid, lp_code);
                        break;
                }
            }

            

        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }

    }
    
    //reward types tier
    static void rewardTypesTier(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Reward Types Tier");
        System.out.println("---------------------------------\n");


        try {

            Scanner sc = new Scanner(System.in);
            // readRewardTypes.setString(1,bid);
            ResultSet result = readRewardTypes.executeQuery();
            // result.beforeFirst();
        
            while(result.next()) {
                System.out.println(result.getString("REWARD_CODE") + ": " + result.getString("REWARD_NAME"));
            }

            System.out.println("0: Go Back");
            System.out.println("Enter the Reward Type Code:");
            // String choice = sc.nextLine();

            String choice = sc.nextLine();

            if (choice=="0") {
                tier(bid, lp_code);
            }
            else {
                addBrandRewardTypesInformation.setString(1, bid);
                addBrandRewardTypesInformation.setString(2, choice);
                addBrandRewardTypesInformation.executeQuery();
                System.out.println("Do you want to add another Reward type?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt();  
                switch(choice2) {
                    case 1:
                        rewardTypesTier(bid, lp_code);
                        break;
                    case 2:
                        tier(bid, lp_code);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        tier(bid, lp_code);
                        break;
                }
            }


        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

// get quantity from user.    

    
//2.add re rules
    static void addRERules(String bid, String lp_code) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Add RE rules");
        System.out.println("---------------------------------\n");
        
        System.out.println("Brand Reward rule code : ");
        String bre_id = sc.nextLine();
        
        System.out.println("Activity category : ");
        String activity_code = sc.nextLine();

        System.out.println("Number of points required : ");
        int n_points = sc.nextInt();
        sc.nextLine();

        System.out.println("1.Add RE Rule\n2.Go Back");
        System.out.println("\nEnter your choice:");

        // Also add version number 

        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Save all the above details in REWARD_EARNING_RULES table
            try {
                    addRERules.setString(1, bre_id);
                    addRERules.setString(2, bid);
                    addRERules.setString(3, activity_code);
                    addRERules.setInt(4, n_points);
                    addRERules.setInt(5, 1);
                    addRERules.executeQuery();

                    addLinkREBrandInfo.setString(1, bre_id);
                    addLinkREBrandInfo.setString(2, bid);
                    addLinkREBrandInfo.setInt(3, 1);
                    addLinkREBrandInfo.executeQuery();
                    
                } 
                catch(SQLException e)
                {
                    e.printStackTrace();
                }
                System.out.println("Reward Earning Rule added!");
                System.out.println("Do you want to add another Reward Earning Rule?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt();  
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        addRERules(bid, lp_code);
                        break;
                    case 2:
                        brandLanding(bid);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        addRERules(bid, lp_code);
                        break;
                }
                break;
            case 2:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addRERules(bid, lp_code);
                break;
        }
    }

    static void updateRERules(String bid, String lp_code) {

        Scanner sc = new Scanner(System.in);
        System.out.println("\n---------------------------------");
        System.out.println("Update RE rules");
        System.out.println("---------------------------------\n");

        System.out.println("Brand Reward rule code : ");
        String bre_id = sc.nextLine();
        
        System.out.println("Activity category : ");
        String activity_code = sc.nextLine();

        System.out.println("Number of points required : ");
        int n_points = sc.nextInt();
        sc.nextLine();

        System.out.println("1.Update RE Rule\n2.Go Back");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();
        int max_version = 1;

        switch(choice) {
            case 1:
                // Update all the above details in REWARD_EARNING_RULES table
            try {
                    findMaxVersionRE.setString(1,bre_id);
                    findMaxVersionRE.setString(2, activity_code);
                    ResultSet res = findMaxVersionRE.executeQuery();
                    if(res.next()) {
                    max_version = res.getInt("max_version");        // or is it getInt(1)?
                    }

                    updateRERulesInformation.setString(1, bre_id);
                    updateRERulesInformation.setString(2, bid);
                    updateRERulesInformation.setString(3, activity_code);
                    updateRERulesInformation.setInt(4, n_points);
                    updateRERulesInformation.setInt(5, max_version + 1);
                    updateRERulesInformation.executeQuery();

                    updateLinkREBrandInfo.setInt(1, max_version + 1);
                    updateLinkREBrandInfo.setString(2, bre_id);
                    updateLinkREBrandInfo.setString(3, bid );
                    updateLinkREBrandInfo.executeUpdate();

                }
                catch (SQLException e) {
                    e.printStackTrace();
                }

                System.out.println("Reward Earning Rules Updated");
                System.out.println("Do you want update Reward Earning Rules again?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt(); 
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        updateRERules(bid, lp_code);
                        break;
                    case 2:
                        brandLanding(bid);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        updateRERules(bid, lp_code);
                        break;
                }
                break;
            case 2:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                updateRERules(bid, lp_code);
                break;
        }
    }

    static void addRRRules(String bid, String lp_code) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Add RR rules");
        System.out.println("---------------------------------\n");

        System.out.println("Brand Reward rule code : ");
        String brr_id = sc.nextLine();
        
        System.out.println("Reward category : ");
        String reward_code = sc.nextLine();

        System.out.println("Number of points required : ");
        int n_points = sc.nextInt();
        sc.nextLine();

        System.out.println("Number of instances : ");
        int n_of_instances = sc.nextInt(); 
        sc.nextLine();       

        System.out.println("1.Add RR Rule\n2.Go Back");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();
    
        switch(choice) {
            case 1:
                // save in database table REWARD_REDEEMING_RULES
            try {
                    addRRRules.setString(1, brr_id);
                    addRRRules.setString(2, bid);
                    addRRRules.setString(3, reward_code);
                    addRRRules.setInt(4, n_points);
                    addRRRules.setInt(5, n_of_instances);
                    addRRRules.setInt(6, 1);
                    addRRRules.executeQuery();

                    addLinkRRBrandInfo.setString(1, brr_id);
                    addLinkRRBrandInfo.setString(2, bid);
                    addLinkRRBrandInfo.setInt(3, 1);
                    addLinkRRBrandInfo.executeQuery();


                } 
                catch(SQLException e)
                {
                    e.printStackTrace();
                }

                System.out.println("Reward Redeeming Rule added!");
                System.out.println("Do you want to add another Reward Redeeming Rule?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt();  
                sc.nextLine();

                switch(choice2) {
                    case 1:
                        addRRRules(bid, lp_code);
                        break;
                    case 2:
                        brandLanding(bid);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        addRRRules(bid, lp_code);
                        break;
                }
                break;
            case 2:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                addRRRules(bid, lp_code);
                break;
        }
    }

    static void updateRRRules(String bid, String lp_code) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Update RR rules");
        System.out.println("---------------------------------\n");

        
        System.out.println("Brand Reward rule code : ");
        String brr_id = sc.nextLine();
        
        System.out.println("Reward category : ");
        String reward_code = sc.nextLine();

        System.out.println("Number of points required : ");
        int n_points = sc.nextInt();
        sc.nextLine();

        System.out.println("Number of instances : ");
        int n_of_instances = sc.nextInt();
        sc.nextLine();

        System.out.println("1.Update RR Rule\n2.Go Back");
        System.out.println("\nEnter your choice:");      
        
        int choice = sc.nextInt();
        sc.nextLine();

        int max_version = 1;
    
        switch(choice) {
            case 1:
                // save all the info in REWARD_REDEEMING_RULES
            try {
                    findMaxVersionRR.setString(1,brr_id);
                    findMaxVersionRR.setString(2, reward_code);
                    ResultSet res = findMaxVersionRR.executeQuery();
                    if(res.next()) {
                     max_version = res.getInt("max_version");        // or is it getInt(1)?
                    }

                    updateRRRulesInformation.setString(1, brr_id);
                    updateRRRulesInformation.setString(2, bid);
                    updateRRRulesInformation.setString(3, reward_code);
                    updateRRRulesInformation.setInt(4, n_points);
                    updateRRRulesInformation.setInt(5, n_of_instances);
                    updateRRRulesInformation.setInt(6, max_version + 1);
                    updateRRRulesInformation.executeQuery();

                    updateLinkRRBrandInfo.setInt(1, max_version + 1);
                    updateLinkRRBrandInfo.setString(2, brr_id);
                    updateLinkRRBrandInfo.setString(3, bid );
                    updateLinkRRBrandInfo.executeUpdate();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }

                System.out.println("Reward Redeeming Rules Updated");
                System.out.println("Do you want update Reward Redeeming Rules again?");
                System.out.println("1.Yes\n2.No.");
                System.out.println("\nEnter your choice:");

                int choice2 = sc.nextInt(); 
                sc.nextLine(); 
                switch(choice2) {
                    case 1:
                        updateRRRules(bid, lp_code);
                        break;
                    case 2:
                        brandLanding(bid);
                        break;
                    default:
                        System.out.println("\nWrong choice. Try again");
                        updateRRRules(bid, lp_code);
                        break;
                }
                
                break;
            case 2:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                updateRRRules(bid, lp_code);
                break;
        }
    }


    //6.validate loyalty program
    static void validateLoyaltyProgram(String bid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Validate Loyalty Program");
        System.out.println("---------------------------------\n");

        System.out.println("1.Validate \n2.Go Back");
    	System.out.println("\nEnter your choice:");
    	Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
    
        switch(choice) {
            case 1:
                // Check if setup was done properly ?????
                // If done properly,
                System.out.println("Validated successfully!");
                brandLanding(bid);
                // If not, print error message.
                // System.Out.println("Error in setting up brand.");
                // brandLanding(bid);
                break;
            case 2:
                brandLanding(bid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                validateLoyaltyProgram(bid, lp_code);
                break;
        }
    }
// print success or error message. Case 1 should go to brand landing after printing message.   
   
   
    //customer landing
    static void customerLanding(String cid) {

        System.out.println("\n---------------------------------");
        System.out.println("Customer Landing");
        System.out.println("---------------------------------\n");

        System.out.println("1.Enroll in Loyalty Program\n2.Reward Activities\n3.View wallet\n4.Redeem points\n5.Log out\n");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // System.out.println("Success!");
                enrollInLoyaltyProgram(cid);
                break;
            case 2:
                rewardActivities(cid);
                break;
            case 3:
                viewWallet(cid);
                break;
            case 4:
                redeemPoints(cid);
                break;
            case 5:
                System.out.print("Logging out..");
                home();                
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                customerLanding(cid);
                break;
            
        }
    }
    
    //enroll In Loyalty Program
    static void enrollInLoyaltyProgram(String cid) {

        Scanner sc = new Scanner(System.in);
        System.out.println("\n---------------------------------");
        System.out.println("Enroll In Loyalty Program");
        System.out.println("---------------------------------\n");

        // select name from loyalty programs.
        // display the list along with index numbers
        // user should enter the index of the lp they want to enroll.

        try {
            ResultSet result = getLoyaltyProgramInformation.executeQuery();
            // result.beforeFirst();
            System.out.println("LP_CODE\tLP_NAME\n");
            while(result.next()) {
                System.out.print(result.getString("LP_CODE") + "\t" + result.getString("LP_NAME") + "\n");
            }
        }
        catch (SQLException e)
        {
            // do something appropriate with the exception, *at least*:
            e.printStackTrace();
        }

            
        System.out.println("Enter LP_CODE to enroll : ");
        String lp_code_selected = sc.nextLine();

        System.out.println("1.Enroll in Loyalty program\n2.Go back\n");
        System.out.println("\nEnter your choice:");
        
        int choice = sc.nextInt();
        sc.nextLine();

        String wallet_id = "";
        String bid = "";

        
        switch(choice) {
            case 1:

                String initial_tier = "null";
                // Check if customer is already enrolled in the lp and save into database
                // check in customer lp tier table
                // if not enrolled, add it to the table.
                // add new entry in customer lp tier table

                try
                {

                    checkifcustomeraleadyinlp.setString(1, cid);
                    checkifcustomeraleadyinlp.setString(2, lp_code_selected);
                    ResultSet res1 = checkifcustomeraleadyinlp.executeQuery();

                    if (!res1.isBeforeFirst()) {
                        // res1.beforeFirst();
                        // call methods that might throw SQLException

                        getWallet_IDforActivities.setString(1, cid);
                        ResultSet res = getWallet_IDforActivities.executeQuery();
                        if(res.next()) {
                            wallet_id = res.getString("wallet_id");
                        }

                        getBIDfromLP.setString(1, lp_code_selected);
                        ResultSet ress = getBIDfromLP.executeQuery();
                        if(ress.next()) {
                            bid = ress.getString("BID");
                        }

                        enrollcustomerinlp.setString(1, cid);
                        enrollcustomerinlp.setString(2, lp_code_selected);


                        findMinTier.setString(1, lp_code_selected);
                        findMinTier.setString(2, lp_code_selected);
                        ResultSet res2 = findMinTier.executeQuery();

                        if (!res2.isBeforeFirst() ) { 
                            // res2.beforeFirst();   
                            
                            enrollcustomerinlp.setString(3, null);

                        }
                        else {

                            // res2.beforeFirst();
                            res2.next();
                            initial_tier = res2.getString("TIER_ID");

                            enrollcustomerinlp.setString(3, initial_tier);
                            
                        }

                        insertbrandwallet.setString(1,wallet_id);
                        insertbrandwallet.setString(2,bid);
                        insertbrandwallet.setString(3,cid);
                        insertbrandwallet.setInt(4,0);
                        insertbrandwallet.executeQuery();

                        enrollcustomerinlp.executeQuery();
                        System.out.println("Success!");
                    }

                    else {
                        System.out.println("Already enrolled!");
                    }
                }
                catch (SQLException e)
                {
                    // do something appropriate with the exception, *at least*:
                    e.printStackTrace();
                }

                    
                customerLanding(cid);
                break;
            case 2:
                customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                enrollInLoyaltyProgram(cid);
                break;
            
        }
    }
// display available loyalty programs and user must delect one before selecting 1 or 2.
// print error message for case 1 if customer is already enrolled in the selected loyalty program.



//reward Activities
    static void rewardActivities(String cid) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Reward Activities");
        System.out.println("---------------------------------\n");

        System.out.println("List of all loyalty programs : ");

        // Display all the loyalty programs enrolled by the user from CUSTOMER_LP_TIER table.

        try
            {

                showLoyaltyProgramsforCustomer.setString(1, cid);
                ResultSet res = showLoyaltyProgramsforCustomer.executeQuery();
                if (!res.isBeforeFirst()) 
                {    
                    System.out.println("Loyalty Programs not found. Please enroll in a Loyalty Program"); 
                    customerLanding(cid);
                }    
                // res.beforeFirst();
                while (res.next()) {
                    System.out.print(res.getString("LP_CODE") + "\n");
                }

            } catch (SQLException e) 
                {
                    e.printStackTrace();
                }
        
        System.out.println("Select loyalty program to perform activity : ");

        String lp_selected = sc.nextLine();

        // Display all ACTIVITY_NAME of ACTIVITY_CODE from ACTIVITY_TYPES
        // The menu will change depending on number of reward activities 

        String bid = "";

        try{
            // Get the bid of lp code from BRAND_LP
            getBIDforRewardActivities.setString(1, lp_selected);
            ResultSet res = getBIDforRewardActivities.executeQuery();
            if(res.next()) {
                bid = res.getString("bid");
                    }
            
            // Get REWARD_CODEs of BID=BID from REWARD_ACTIVITY
            getActivityCodesforRewardActivities.setString(1, bid);
            res = getActivityCodesforRewardActivities.executeQuery();
            while (res.next()) {
                System.out.print(res.getString("ACTIVITY_CODE") + "\t" + res.getString("ACTIVITY_NAME") + "\n");
                    }     

        }
        catch (SQLException e) 
        {
            e.printStackTrace();
        }

        System.out.println("\n Enter 0 to go back.");

        System.out.println("\n Enter Activity Code of your choice : ");
        String a_code = sc.nextLine();

        //System.out.println("1.Purchase\n2.Leave a review\n3.Refer a friend\n4.Go back\n");
        //System.out.println("\nEnter your choice:");
        
        //int choice = sc.nextInt();
        
        switch(a_code) {
            case "A01":
                purchase(cid,lp_selected);
                break;
            case "A02":
                leaveAReview(cid,lp_selected);
                break;
            case "A03":
                referAFriend(cid,lp_selected);
                break;
            case "0":
              customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                rewardActivities(cid);
                break;
        }
    }
// display list of joined loyalty programs


    //purchase
        static void purchase(String cid, String lp_code) {

            Scanner sc = new Scanner(System.in);

            System.out.println("\n---------------------------------");
            System.out.println("Purchase");
            System.out.println("---------------------------------\n");

            // Gift card ?? // insert into loyalty_activity, wallet_activity for purchase based on tier, reward_activity only gift card, wallet 

            System.out.println("Do you want to use Gift card?\n1.Yes\n2.No\n");
            int use_gift_card = sc.nextInt();
            sc.nextLine();
            
            System.out.println("Enter date of purchase : ");
            String date = sc.nextLine();


            System.out.println("1.Purchase\n2.Go back\n");
            System.out.println("\nEnter your choice:");
            
            int choice = sc.nextInt();
            sc.nextLine();

            ResultSet res;
            String bid = "";
            String brr_id = "";
            String bre_id = "";
            int no_of_points = 0;
            int points = 0;
            int points_gained = 0;
            Date redeem_date = Date.valueOf(date);

            int multiplier = 1;
            
            switch(choice) {
                case 1:
                    
                    try {
                    
                    if (use_gift_card == 1) {
                        // Get gift card points from REWARD_REDEEMING_RULES table
                        // Record gift card activity in REWARD_ACTIVITY table
                        // To find BID of lp_code in order to get bre_id and thereby number of points for this activity
                        getBIDforNum_Of_PointsPUR.setString(1, lp_code);
                        res = getBIDforNum_Of_PointsPUR.executeQuery();
                        if(res.next()) {
                            bid = res.getString("bid");
                        }

                        // getBrrid.setString(1, bid);
                        // res = getBrrid.executeQuery();
                        // if(res.next()) {
                        //     brr_id = res.getString("B_RR_ID");
                        // }

                        getPointsforRRR.setString(1, bid);
                        getPointsforRRR.setString(2, "R01");
                        res = getPointsforRRR.executeQuery();
                        if(res.next()) {
                            points = res.getInt("points");
                        }

                        // System.out.println("\nEnter date:");
                        // String entry_date = sc.nextLine();

                        // redeem_date = Date.valueOf(entry_date);

                        // Insert into Reward_Activity

                        addReward_Activity.setString(1, null);
                        addReward_Activity.setString(2, cid);
                        addReward_Activity.setString(3, bid);
                        addReward_Activity.setString(4, "R01");
                        addReward_Activity.setInt(5, points);
                        addReward_Activity.setDate(6, redeem_date);
                        addReward_Activity.executeQuery();


                        // To decrese the number of instances in the Reward_Redeeming_Rules
                        updateNoOfInstancesPUR.setString(1, brr_id);
                        updateNoOfInstancesPUR.executeQuery();

                        System.out.println("Using giftcard for purchase");
                    }

                    // Get purchase points from REWARD_EARNING_RULES table
                    // Get customer's tier with that loyalty program from CUSTOMER_LP_TIER
                    // Get multiplier value from LOYALTY_PROGRAM_TIERS table based on customer's tier 

                    // Customer gains points equal to the value of the activity*multiplier

                    // Record activity details into LOYALTY_ACTIVITY table
                    
                        // To find multiplier 
                        findMultiplierPUR.setString(1, cid);
                        findMultiplierPUR.setString(2, lp_code);
                        res = findMultiplierPUR.executeQuery();
                        if(res.next()) {
                            multiplier = res.getInt("multiplier");
                        }

                        System.out.println("Multiplier : " + String.valueOf(multiplier));


                        // To find BID of lp_code in order to get bre_id and thereby number of points for this activity
                        getBIDforNum_Of_PointsPUR.setString(1, lp_code);
                        res = getBIDforNum_Of_PointsPUR.executeQuery();
                        if(res.next()) {
                            bid = res.getString("bid");
                        }

                        // // To find B_RE_ID for given BID in order to get number of points for activity code
                        // getbre_idforNum_Of_PointsPUR.setString(1, bid);
                        // res = getbre_idforNum_Of_PointsPUR.executeQuery();
                        // if(res.next()) {
                        //     bre_id = res.getString("bre_id");
                        // }

                        // Get number of points for given activity code= 'A01' and bre_id
                        // getNumber_Of_PointsPUR.setString(1, bre_id);
                        getNumber_Of_PointsPUR.setString(1, bid);
                        getNumber_Of_PointsPUR.setString(2, "A01");
                        res = getNumber_Of_PointsPUR.executeQuery();
                        if(res.next()) {
                            no_of_points = res.getInt("no_of_points");
                        }


                        System.out.println("No of points : " + String.valueOf(no_of_points));

                        // Customer gains points equal to 2 value of the activity*multiplier
                        points_gained = no_of_points * multiplier;


                        System.out.println("Points gained : " + String.valueOf(points_gained));


                        // System.out.println("Enter something about purchase : "); // what should the content be?
                        // Scanner sc = new Scanner(System.in);
                        // String purchase_content= sc.nextLine();

                        addLoyalty_ActivityPUR.setString(1, null);
                        addLoyalty_ActivityPUR.setString(2, cid);
                        addLoyalty_ActivityPUR.setString(3, bid);
                        addLoyalty_ActivityPUR.setString(4, "A01");
                        addLoyalty_ActivityPUR.setInt(5, points_gained);  

                        // System.out.println("\nEnter date:");
                        // String join_date = sc.nextLine();

                        // Date sqldate = Date.valueOf(join_date);

                        // java.util.Date today = new java.util.Date();
                        // java.sql.Date sqldate = new java.sql.Date(today.getTime());


                        addLoyalty_ActivityPUR.setDate(6, redeem_date);    // Get Date: Maybe make the column itself insert system date automatically when you insert a value currently this gives date and time.
                        addLoyalty_ActivityPUR.setString(7, "PURCHASE");
                        addLoyalty_ActivityPUR.executeQuery();

                        // To get Wallet_id from WALLET table
                        getWallet_IDforPUR.setString(1, cid);
                        res = getWallet_IDforPUR.executeQuery();
                        String wallet_id = "";
                        if(res.next()) {
                            wallet_id = res.getString("wallet_id");
                        }

                        addWallet_ActivityInfo.setString(1, null);
                        addWallet_ActivityInfo.setString(2, wallet_id);
                        addWallet_ActivityInfo.setDate(3, redeem_date);
                        addWallet_ActivityInfo.setString(4, bid);
                        addWallet_ActivityInfo.setString(5, "A01");
                        addWallet_ActivityInfo.setInt(6, points_gained);
                        addWallet_ActivityInfo.executeQuery();

                        // getPoints_Earned.setString(1, wallet_id);
                        // getPoints_Earned.setString(2, cid);
                        // res = getPoints_Earned.executeQuery();
                        // int pts = 0;
                        // if(res.next()) {
                        //     pts = res.getInt("pts");
                        // }
                        // int total_points = pts +  points_gained;

                        updateWalletInfo.setInt(1, points_gained);
                        updateWalletInfo.setString(2, wallet_id);
                        updateWalletInfo.setString(3, cid);
                        updateWalletInfo.executeUpdate();

                        updateBrandWalletInfo.setInt(1, points_gained);
                        updateBrandWalletInfo.setString(2, cid);
                        updateBrandWalletInfo.setString(3, bid);
                        updateBrandWalletInfo.executeUpdate();

                    } catch(SQLException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Purchase successful");   

                    rewardActivities(cid);
                    break;
                case 2:
                    customerLanding(cid);
                    break;
                default:
                    System.out.println("\nWrong choice. Try again");
                    purchase(cid, lp_code);
                    break;
            }
        }
// ask user to select gift card if any.
// take input from user about purchase (check sql table)


    //leave a Review
    static void leaveAReview(String cid, String lp_code) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Leave a Review");
        System.out.println("---------------------------------\n");

        System.out.println("Enter date of review : ");
        String date = sc.nextLine();
        java.sql.Date sqldate = java.sql.Date.valueOf(date);

        System.out.println("Enter review content : ");
        String content = sc.nextLine();

        System.out.println("1.Leave a review\n2.Go back\n");
        System.out.println("\nEnter your choice:");
        int choice = sc.nextInt();
        sc.nextLine();

        ResultSet res;
        String bid = "";
        String bre_id = "";
        int no_of_points = 0;
        int points_gained = 0;
        String wallet_id = "";

        int multiplier = 1;

        
        switch(choice) {
            case 1:
                // Record activity details along with content into LOYALTY_ACTIVITY table
                // Customer gains points equal to the value of the activity*multiplier
                try{
                    findMultiplierLAR.setString(1, cid);
                    findMultiplierLAR.setString(2, lp_code);
                    res = findMultiplierLAR.executeQuery();
                    if(res.next()) {
                        multiplier = res.getInt("multiplier");
                    }

                    System.out.println("Multiplier : " + String.valueOf(multiplier));

                    // To find BID of lp_code in order to get bre_id and thereby number of points for this activity
                    getBIDforNum_Of_PointsLAR.setString(1, lp_code);
                    res = getBIDforNum_Of_PointsLAR.executeQuery();
                    if(res.next()) {
                    bid = res.getString("bid");
                    }

                    // // To find B_RE_ID for given BID in order to get number of points for activity code
                    // getbre_idforNum_Of_PointsLAR.setString(1, bid);
                    // res = getbre_idforNum_Of_PointsLAR.executeQuery();
                    // if(res.next()) {
                    //     bre_id = res.getString("bre_id");
                    //     System.out.println("BRE_ID : " + bre_id);

                    // }

                    // Get number of points for given activity code and bre_id
                    getNumber_Of_PointsLAR.setString(1, bid);
                    getNumber_Of_PointsLAR.setString(2, "A02");

                    res = getNumber_Of_PointsLAR.executeQuery();
                    if(res.next()) {
                        no_of_points = res.getInt("no_of_points");
                    }

                    System.out.println("No of points : " + String.valueOf(no_of_points));


                    // Customer gains points equal to the value of the activity*multiplier
                    points_gained = no_of_points * multiplier;

                    System.out.println("Points gained : " + String.valueOf(points_gained));

                    addLoyalty_ActivityLAR.setString(1, null);
                    addLoyalty_ActivityLAR.setString(2, cid);
                    addLoyalty_ActivityLAR.setString(3, bid);
                    addLoyalty_ActivityLAR.setString(4, "A02");
                    addLoyalty_ActivityLAR.setInt(5, points_gained); 
                    addLoyalty_ActivityLAR.setDate(6, sqldate); 
                    addLoyalty_ActivityLAR.setString(7, content);
                    addLoyalty_ActivityLAR.executeQuery();

                    // To get Wallet_id from WALLET table
                    getWallet_IDforActivities.setString(1, cid);
                    res = getWallet_IDforActivities.executeQuery();
                    if(res.next()) {
                        wallet_id = res.getString("wallet_id");
                    }

                    // Updating in Wallet and Wallet_Activity
                    updateWalletInfo.setInt(1, points_gained);
                    updateWalletInfo.setString(2, wallet_id);
                    updateWalletInfo.setString(3, cid);
                    updateWalletInfo.executeUpdate();

                    updateBrandWalletInfo.setInt(1, points_gained);
                    updateBrandWalletInfo.setString(2, cid);
                    updateBrandWalletInfo.setString(3, bid);
                    updateBrandWalletInfo.executeUpdate();

                    addWallet_ActivityInfo.setString(1, null);
                    addWallet_ActivityInfo.setString(2, wallet_id);
                    addWallet_ActivityInfo.setDate(3, sqldate);
                    addWallet_ActivityInfo.setString(4, bid);
                    addWallet_ActivityInfo.setString(5, "A02");
                    addWallet_ActivityInfo.setInt(6, points_gained);
                    addWallet_ActivityInfo.executeQuery();


                } catch(SQLException e) {
                    e.printStackTrace();
                }
                
                System.out.println("Thank you for your valuable review");

                rewardActivities(cid);
                break;
            case 2:
                customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                leaveAReview(cid, lp_code);
                break;
        }
    }
// get review info from user.

    
//refer a friend
     static void referAFriend(String cid, String lp_code) {

        System.out.println("\n---------------------------------");
        System.out.println("Refer a friend");
        System.out.println("---------------------------------\n");

        System.out.println("1.Refer\n2.Go back\n");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();

        ResultSet res;
        String bid = "";
        String bre_id = "";
        int no_of_points = 0;
        int points_gained = 0;
        String wallet_id = "";

        int multiplier = 1;
        
        switch(choice) {
            case 1:
                try {
                    // Customer gains points equal to the value of the activity*multiplier
                    // To find multiplier 
                    findMultiplierRAF.setString(1, cid);
                    findMultiplierRAF.setString(2, lp_code);
                    res = findMultiplierRAF.executeQuery();
                    if(res.next()) {
                        multiplier = res.getInt("multiplier");
                    }

                    System.out.println("Multiplier : " + String.valueOf(multiplier));


                    // To find BID of lp_code in order to get bre_id and thereby number of points for this activity
                    getBIDforNum_Of_PointsRAF.setString(1, lp_code);
                    res = getBIDforNum_Of_PointsRAF.executeQuery();
                    if(res.next()) {
                        bid = res.getString("bid");
                    }

                    // // To find B_RE_ID for given BID in order to get number of points for activity code
                    // getbre_idforNum_Of_PointsRAF.setString(1, bid);
                    // res = getbre_idforNum_Of_PointsRAF.executeQuery();
                    // if(res.next()) {
                    //     bre_id = res.getString("bre_id");
                    // }

                    // Get number of points for given activity code and bre_id
                    getNumber_Of_PointsRAF.setString(1, bid);
                    getNumber_Of_PointsRAF.setString(2, "A03");
                    res = getNumber_Of_PointsRAF.executeQuery();
                    if(res.next()) {
                        no_of_points = res.getInt("no_of_points");
                    }

                    System.out.println("No of points : " + String.valueOf(no_of_points));


                    // Customer gains points equal to the value of the activity*multiplier
                    points_gained = no_of_points * multiplier;

                    System.out.println("Points gained : " + String.valueOf(points_gained));

                    System.out.println("Enter the Friend's Name to be stored : ");
                    // Scanner sc = new Scanner(System.in);
                    String friend_name= sc.nextLine();

                    addLoyalty_ActivityRAF.setString(1, null);
                    addLoyalty_ActivityRAF.setString(2, cid);
                    addLoyalty_ActivityRAF.setString(3, bid);
                    addLoyalty_ActivityRAF.setString(4, "A03");
                    addLoyalty_ActivityRAF.setInt(5, points_gained);  

                    System.out.println("\nEnter date:");
                    String join_date = sc.nextLine();

                    Date sqldate = Date.valueOf(join_date);

                    // java.util.Date today = new java.util.Date();
                    // java.sql.Date sqldate = new java.sql.Date(today.getTime());


                    addLoyalty_ActivityRAF.setDate(6, sqldate);    // Get Date: Maybe make the column itself insert system date automatically when you insert a value currently this gives date and time.
                    addLoyalty_ActivityRAF.setString(7, friend_name );
                    addLoyalty_ActivityRAF.executeQuery();

                    // To get Wallet_id from WALLET table
                    getWallet_IDforActivities.setString(1, cid);
                    res = getWallet_IDforActivities.executeQuery();
                    if(res.next()) {
                        wallet_id = res.getString("wallet_id");
                    }

                    // Updating in Wallet and Wallet_Activity
                    updateWalletInfo.setInt(1, points_gained);
                    updateWalletInfo.setString(2, wallet_id);
                    updateWalletInfo.setString(3, cid);
                    updateWalletInfo.executeUpdate();

                    updateBrandWalletInfo.setInt(1, points_gained);
                    updateBrandWalletInfo.setString(2, cid);
                    updateBrandWalletInfo.setString(3, bid);
                    updateBrandWalletInfo.executeUpdate();

                    addWallet_ActivityInfo.setString(1, null);
                    addWallet_ActivityInfo.setString(2, wallet_id);
                    addWallet_ActivityInfo.setDate(3, sqldate);
                    addWallet_ActivityInfo.setString(4, bid);
                    addWallet_ActivityInfo.setString(5, "A03");
                    addWallet_ActivityInfo.setInt(6, points_gained);
                    addWallet_ActivityInfo.executeQuery();

                } catch(SQLException e) {
                    e.printStackTrace();
                }
                    
                System.out.println("Thank you for your referral.");
                referAFriend(cid, lp_code);
                break;

            case 2:
                customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                referAFriend(cid, lp_code);
                break;
        }
    }


// customer gains points after referring


// customer gains points after referring

    //view wallet
    //view wallet
    static void viewWallet(String cid) {

        System.out.println("\n---------------------------------");
        System.out.println("View Wallet");
        System.out.println("---------------------------------\n");

        // select * from wallet where cid=cid and groupby bid
        // display everything
        try {
            showWalletInfo.setString(1, cid);
            ResultSet res = showWalletInfo.executeQuery();
            if (!res.isBeforeFirst()) 
                {    
                    System.out.println("Wallet not found."); 
                }
            // res.beforeFirst();
            System.out.print("POINTS_EARNED\n");
            while (res.next()) {
            System.out.print(res.getInt("POINTS_EARNED") + "\n");
                }

            } catch (SQLException e) 
                {
                    e.printStackTrace();
                }

        System.out.println("\n1.Go back\n");
        System.out.println("\nEnter your choice:");
        Scanner sc = new Scanner(System.in);
        int choice = sc.nextInt();
        sc.nextLine();
        
        // Display wallet content

        switch(choice) {
            case 1:
                customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                viewWallet(cid);
                break;
        }
    }

//redeem points
    static void redeemPoints(String cid) {

        Scanner sc = new Scanner(System.in);

        System.out.println("\n---------------------------------");
        System.out.println("Redeem Points");
        System.out.println("---------------------------------\n");

        System.out.println("List of all reward types");
        // Display all the reward options from REWARD_REDEEMING_RULES, REWARD_TYPES table for the brand (LINK_RR_BRAND)

        System.out.println("\nB_RR_ID\tREWARD_CODE\tNO_OF_POINTS\tNO_OF_INSTANCES");
       
        try {
            //set cid
            getLPCode.setString(1,cid);
            ResultSet result1 = getLPCode.executeQuery();
            //result1.beforeFirst();

            //For each brand
            while(result1.next()) {

                //get LP_CODE
                String lpcode = result1.getString("LP_CODE");
                
                //set LP_CODE
                getBID.setString(1, lpcode);
                ResultSet result2 = getBID.executeQuery();

                //get BID
                result2.next();
                String bid = result2.getString("BID");

                //set BID
                getBRRID.setString(1,bid);
                ResultSet result3 = getBRRID.executeQuery();
               

                //for each RewardRedeemingRule
                while(result3.next()) {
                        
                        //get B_RR_ID
                        String brrid = result3.getString("B_RR_ID");

                        //set B_RR_ID
                        displayRewardRedeemingRules.setString(1, brrid);
                        ResultSet result4 = displayRewardRedeemingRules.executeQuery();
                        result4.next();
                       
                        
                        //display
                        System.out.println(result4.getString("B_RR_ID") + "\t" + result4.getString("REWARD_CODE") + "\t" + result4.getString("NO_OF_POINTS") + "\t" + result4.getString("NO_OF_INSTANCES") + "\n");

                }
            }
        }
        catch (SQLException e)
            {
                e.printStackTrace();
            }


        System.out.println("1.Reward Selection\n2.Go back\n");
        System.out.println("\nEnter your choice:");
        int choice = sc.nextInt();
        sc.nextLine();
        
        switch(choice) {
            case 1:
                // Check RRRules ad record the redeem activity and insert into database
                try {
                    System.out.println("\nChoose Brand Reward Code (B_RR_ID): ");
                    String brridSelected = sc.nextLine();
  

                    getRewardInformation.setString(1,brridSelected);
                    ResultSet result5 = getRewardInformation.executeQuery();
                    result5.next();
                    
                    //get points required to redeem and the number of instances
                    String rewardCodeString = result5.getString("REWARD_CODE");
                    int pointsRequired = result5.getInt("NO_OF_POINTS");
                    int noOfInstances = result5.getInt("NO_OF_INSTANCES");

                    //get bid using brrid
                    getBIDUsingBRRID.setString(1,brridSelected);
                    ResultSet result7 = getBIDUsingBRRID.executeQuery();
                    result7.next();
                    String bidSelected = result7.getString("BID");

                    //set cid and bid to get wallet info
                    getBrandWalletInfo.setString(1,cid);
                    getBrandWalletInfo.setString(2,bidSelected);
                    //etWalletInfo.setString(2,bidSelected);
                    ResultSet result6 = getBrandWalletInfo.executeQuery();
                    result6.next();

                    //get no of points;
                    int pointsEarned = result6.getInt("LP_POINTS_EARNED");

                    


                    //check if the customer has enough points
                    if( pointsEarned >= pointsRequired && noOfInstances > 0 ) {

                        //reduce the points in the customer's wallet
                        int remainingPoints = pointsEarned - pointsRequired;                       
                        updateWalletPoints.setInt(1,remainingPoints);
                        updateWalletPoints.setString(2,cid);
                        //updateWalletPoints.setString(3,bidSelected);
                        updateWalletPoints.executeQuery();

                        updateBrandWalletInfo.setInt(1, -pointsRequired);
                        updateBrandWalletInfo.setString(2, cid);
                        updateBrandWalletInfo.setString(3, bidSelected);
                        updateBrandWalletInfo.executeUpdate();



                        //update number of instances
                        updateNoOfInstances.setString(1,brridSelected);
                        updateNoOfInstances.executeQuery();

                        //update reward activity table
                        updateRewardActivity.setString(1,cid);
                        updateRewardActivity.setString(2,bidSelected);
                        updateRewardActivity.setString(3,rewardCodeString);
                        updateRewardActivity.setInt(4,pointsRequired);

                        System.out.println("\nEnter date:");
                        String join_date = sc.nextLine();

                        Date sqldate = Date.valueOf(join_date);


                        updateRewardActivity.setDate(5,sqldate);
                        updateRewardActivity.executeQuery();


                        System.out.println("\nYou have succesfully redeemed this reward!");
                    }

                    else {
                        System.out.println("\nYou do not have enough points to redeem this reward!");
                        redeemPoints(cid);
                    }

                }
                catch (SQLException e)
                {
                    e.printStackTrace();
                }

                redeemPoints(cid);
                break;
            case 2:
                customerLanding(cid);
                break;
            default:
                System.out.println("\nWrong choice. Try again");
                redeemPoints(cid);
                break;
        }
    }    
// display reward for user to select  
// get quantity of reward  


    //2.add re rules
        static void logout() {
            System.out.println("\n---------------------------------");
            System.out.println("Log out");
            System.out.println("---------------------------------\n");
            // Logout
            try {
                conn.close();
            }
            catch (Exception ex) {
                    ex.printStackTrace();
            }
            System.out.println("Logging out!");
        }
    
    //main function
	public static void main(String[] args) {
		//System.out.println("Hello World");
		try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection conn = DriverManager.getConnection(jdbcURL, user, password);
            CreatePreparedStatements(conn);

            // stmt = conn.createStatement();
            // stmt.executeUpdate("alter session set NLS_DATE_FORMAT = 'MM/DD/YYYY';");
            
            home();
            conn.close();
        } 
        catch (ClassNotFoundException ex) {
                    ex.printStackTrace();
        } catch (SQLException ex) {
                    ex.printStackTrace();
        }
                
	}
}
