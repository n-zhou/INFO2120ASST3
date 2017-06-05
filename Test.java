public HashMap<String,Object> makeBooking(String byStaff, String forMember, String vehicle, Date departs) throws OlympicsDBException {
       HashMap<String,Object> booking = null;

       booking = new HashMap<String,Object>();
       PreparedStatement stmt1 = null;
       PreparedStatement stmt2 = null;
       Connection conn = null;
       try{
         conn = getConnection();

         //String query = "SELECT journey_id FROM Journey WHERE vehicle_code = '" + vehicle + "' AND depart_time = '" + departs + "'";
         PreparedStatement stmt = conn.prepareStatement("SELECT journey_id, nbooked, capacity "
             + "FROM Journey JOIN Vehicle USING (vehicle_code) "
             + "WHERE vehicle_code = ? AND depart_time = ?");
         stmt.setString(1, vehicle);
         java.sql.Timestamp timestamp = new java.sql.Timestamp(departs.getTime());
         stmt.setTimestamp(2, timestamp);
         ResultSet rset = stmt.executeQuery();
         int journey_id = 0;
         int nbooked = 0;
         int capicity = 0;
         if(rset.next()){
           journey_id = rset.getInt(1);
           nbooked = rset.getInt(2);
           capicity = rset.getInt(3);
         }
         stmt.close();
         conn.setAutoCommit(false);
         if(nbooked < capicity){

           stmt2 = conn.prepareStatement("INSERT INTO Booking VALUES(?, ?, ?, ?); "
               + "UPDATE Journey SET nbooked = ? WHERE journey_id = ?;");
           stmt2.setString(1, forMember);
           stmt2.setString(2, byStaff);
           stmt2.setTimestamp(3, timestamp);
           stmt2.setInt(4, journey_id);
           int newnbooked = nbooked + 1;
           stmt2.setInt(5, newnbooked);
           stmt2.setInt(6, journey_id);
           int update = stmt2.executeUpdate();
           if(update != 0){
             conn.commit();

           }else{
             try {
           conn.rollback();
         } catch (SQLException e1) {
           e1.printStackTrace();
         }
           }
           booking = getBookingDetails(forMember, journey_id);
         }else{
           try {
         conn.rollback();
       } catch (SQLException e1) {
         e1.printStackTrace();
       }
         }
         conn.setAutoCommit(true);
         stmt2.close();
         rset.close();
         conn.close();
       }catch (Exception e){
         if(conn != null){
           try {
         conn.rollback();
       } catch (SQLException e1) {
         e1.printStackTrace();
       }
         }
         throw new OlympicsDBException(e.getMessage());
       }

       return booking;
      }
