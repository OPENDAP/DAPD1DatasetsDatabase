/**
 *  Copyright: 2014 OpenDAP, Inc.
 *
 * Author: James Gallagher <jgallagher@opendap.org>
 * 
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * You can contact OpenDAP, Inc. at PO Box 112, Saunderstown, RI. 02874-0112.
 */
package org.opendap.d1.DatasetsDatabase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dspace.foresite.OREException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.ResourceMap;
//import java.io.InputStream;
//import java.security.NoSuchAlgorithmException;

/**
 * @brief A database of stuff for the DAP/D1 servlet.
 * 
 * This class is an interface to a SQL database that holds information about
 * DAP-accessible that should also be accessible using DataONE. In it's 
 * initial form it will use SQLite.
 * 
 * This version assumes that the SDO will be a netCDF file and the SMO will be
 * a ISO 19115 document. It uses the current time as 'date added or modified.'
 *  
 * @author James Gallagher
 *
 */
public class DatasetsDatabase {

	/// This is added the the URL to form part of the unique identifier for a D1 SDO
	public static String SDO_IDENT = "sdo";
	/// DAP URL extension for a SDO
	public static String SDO_EXT = ".nc";
	/// SDO D1 formatID
	public static String SDO_FORMAT = "netcdf";

	/// D1 SMO
	public static String SMO_IDENT = "smo";
	/// DAP URL extension for a SMO
	public static String SMO_EXT = ".iso";
	/// SMO D1 formatID
	public static String SMO_FORMAT = "iso19115";
	
	/// D1 ORE
	public static String ORE_IDENT = "ore";
	/// ORE D1 formatID
	public static String ORE_FORMAT = "http://www.openarchives.org/ore/terms/";

	private static Logger log = Logger.getLogger(DatasetsDatabase.class);
	
	private static String dbName = "";
	private Connection c = null;
	
	/**
	 * Open a connection to the database. Note that the database is kept open until
	 * the object goes out of scope, when its finalize() method closes the connection.
	 * 
	 * @param name The name of the database file
	 * @exception Exception is thrown if the sqlite class is not found or the 
	 * connection cannot be opened.
	 * 
	 */
	public DatasetsDatabase(String name) throws SQLException, ClassNotFoundException {
		dbName = name;
		
		try {
			// load the sqlite-JDBC driver using the current class loader
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:" + dbName);
		} catch (SQLException e) {
			log.error("Failed to open database (" + dbName + ").");
			throw e;
		} catch (ClassNotFoundException e) {
			log.error("Failed to load the SQLite JDBC driver.");
			throw e;
		}
		    
		log.debug("Opened database successfully (" + dbName + ").");
	}

	/** 
	 * Closes the DB connection 
	 * @note Not sure if this is needed... Connection might take care of it.
	 */
	protected void finalize( ) throws Throwable {
		c.close();
		log.debug("Database connection closed (" + dbName + ").");
		super.finalize();
	}
	 
	/**
	 * Build the tables, assumes the DB is empty.
	 * 
	 * Note also that the foreign key constraints have been removed because
	 * sqlite on my computer does not seem to support them or the syntax is odd
	 * or something. See the SQLite docs at
	 * http://www.sqlite.org/foreignkeys.html for more info on this including to
	 * compile SQLite so that it supports foreign keys.
	 * 
	 * @throws SQLException Thrown if the tables already exist
	 */
	protected void initTables() throws SQLException {
		Statement stmt = c.createStatement();

		try {
			String sql = "CREATE TABLE Metadata "
					+ "(Id			TEXT NOT NULL," // FOREIGN KEY; sqlite might not support this
					+ " dateAdded 	TEXT NOT NULL,"
					+ " serialNumber INT NOT NULL,"
					+ " format 		TEXT NOT NULL,"
					+ " size 		TEXT NOT NULL,"
					+ " checksum 	TEXT NOT NULL,"
					+ " algorithm 	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			sql = "CREATE TABLE ORE "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " SMO_Id 	TEXT NOT NULL,"
					+ " SDO_Id 	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			sql = "CREATE TABLE SDO "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " DAP_URL	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			sql = "CREATE TABLE SMO "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " DAP_URL	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
		} catch (SQLException e) {
			log.error("Failed to create new database tables (" + dbName + ").");
			throw e;
		} finally {
			stmt.close();			
		}

		log.debug("Made database tables successfully (" + dbName + ").");
	}
	
	public boolean isValid() throws SQLException {
		final Set<String> tableNames 
			= new HashSet<String>(Arrays.asList("Metadata", "ORE", "SMO", "SDO"));
		
		Statement stmt = c.createStatement();
		String sql = "SELECT name FROM sqlite_master WHERE type='table';";
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(sql);
			int count = 0;
			while (rs.next()) {
				count++;
				String name = rs.getString("name");
				if (!tableNames.contains(name)) {
					log.error("Database failed validity test; does not have table: " + name);
					return false;
				}
			}
			if (count != tableNames.size()) {
				log.error("Database failed validity test; does not have all the required tables.");
				return false;
			}
			
			// All tests passed
			return true;
		} catch (SQLException e) {
			log.error("Error querying the database (" + dbName + ").");
			throw e;
		}
		finally {
			rs.close();
			stmt.close();
		}		
	}
	
	/**
	 * Is the PID in the database? Lookup the PID in the datbase's Metadata
	 * table and return true if it's found.
	 * 
	 * @param pid The D1 PID to look for
	 * @return True if found.
	 * @throws DAPDatabaseException 
	 * @throws SQLException 
	 */
	public boolean isInMetadata(String pid) {
		try {
			return getTextMetadataItem(pid, "Id") != null;
		} catch (SQLException e) {
			log.error("Error querying the database (" + dbName + ") for PID: " + pid + ".");
			return false;
		} catch (DAPDatabaseException e) {
			log.error("Error querying the database (" + dbName + ") for PID: " + pid + ".");
			return false;
		}
	}
	
	/**
	 * This version of loadDataset assumes that the SDO will be a netCDF file
	 * and the SMO will be a ISO 19115 document. It uses the current time as
	 * 'date added or modified.'
	 * 
	 * @param URL This is the base URL of the DAP Access point. DAP2 assumed.
	 * @throws SQLException
	 */
	protected void addDataset(String URL) throws SQLException, Exception {
		c.setAutoCommit(false);
		Statement stmt = c.createStatement();
	
		try {
			// Use this ISO601 time string for all three entries
			String now8601 = ISO8601(new Date());
			Long serialNumber = new Long(1);	// when calling, dataset is always new
			
			// First add the SDO info
			String SDO = buildId(URL, SDO_IDENT, serialNumber);	// reuse
			String SDOURL = buildDAPURL(URL, SDO_EXT);
			String sql = "INSERT INTO SDO (Id, DAP_URL) VALUES ('" + SDO + "','" + SDOURL + "');";
			stmt.executeUpdate(sql);
			
			CountingInputStream cis = getDAPURLContents(SDOURL);
			Checksum checksum = ChecksumUtil.checksum(cis, "SHA-1");
			Long size = new Long(cis.getByteCount());
			cis.close();
			
			insertMetadata(stmt, now8601, serialNumber, SDO, SDO_FORMAT, checksum, size);

			// Then add the SMO info
			String SMO = buildId(URL, SMO_IDENT, serialNumber);	// reuse
			String SMOURL = buildDAPURL(URL, SMO_EXT);
			sql = "INSERT INTO SMO (Id, DAP_URL) VALUES ('" + SMO + "', '" + SMOURL + "');";
			stmt.executeUpdate(sql);

			cis = getDAPURLContents(SMOURL);
			checksum = ChecksumUtil.checksum(cis, "SHA-1");
			size = new Long(cis.getByteCount());
			cis.close();
			
			insertMetadata(stmt, now8601, serialNumber, SMO, SMO_FORMAT, checksum, size);

			// Then add the ORE info
			String ORE = buildId(URL, ORE_IDENT, serialNumber);
			sql = "INSERT INTO ORE (Id, SDO_Id, SMO_Id) VALUES ('" + ORE + "', '" + SDO + "', '" + SMO + "');";
			stmt.executeUpdate(sql);

			cis = getOREDocContents(ORE, SMO, SDO);
			checksum = ChecksumUtil.checksum(cis, "SHA-1");
			size = new Long(cis.getByteCount());
			cis.close();
			
			insertMetadata(stmt, now8601, serialNumber, ORE, ORE_FORMAT, checksum, size);

		} catch (SQLException e) {
			log.error("Failed to load values into new database tables (" + dbName + ").");
			throw e;
		} finally {
			stmt.close();
			c.commit();
		}
	}

	/**
	 * Convenience method to set columns in the creatively-named 'Metadata' table.
	 * 
	 * @param stmt
	 * @param now8601
	 * @param servialNumber
	 * @param PID
	 * @param format
	 * @param checksum
	 * @param size
	 * @throws SQLException
	 */
	private void insertMetadata(Statement stmt, String now8601, Long serialNumber, String PID, String format, Checksum checksum, Long size) 
			throws SQLException {
		String sql = "INSERT INTO Metadata (Id,dateAdded,serialNumber,format,size,checksum,algorithm) "
				+ "VALUES ('" + PID + "','" + now8601 + "','" + serialNumber.toString() + "','" + format 
				+ "','" + size.toString() + "','" + checksum.getValue() + "','" + checksum.getAlgorithm() + "');";
		log.debug("SQL Statement: " + sql);
		stmt.executeUpdate(sql);
	}
	
	/**
	 * Get a CountingInputStream that contains the DAP URL's contents. This kind of InputStream
	 * can be used with D1's ChecksumUtil class to compute the checksum and then us to find 
	 * the number of bytes. This way the checksum and size can be computed without reading the 
	 * object twice.
	 * 
	 * @param URL
	 * @return An instance of CountingInputStream
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	private CountingInputStream getDAPURLContents(String URL) throws ClientProtocolException, IOException {
		HttpClient client = new DefaultHttpClient();
		HttpGet request = new HttpGet(URL);
		HttpResponse response = client.execute(request);

		// Get the response
		return new CountingInputStream(response.getEntity().getContent());
	}
		
	/**
	 * Instead of reading the ORE doc - because they are built on-the-fly - make
	 * it and return a CountingInputStream that can be used to both compute the
	 * checksum and count the bytes.
	 * 
	 * @param ORE A String that holds the D1 PID for the ORE document 
	 * @param SMO A String that holds the D1 PID for the SMO document
	 * @param SDO A String that holds the D1 PID for the SDO document
	 * @return An instance of CountingInputStream
	 * @throws OREException
	 * @throws URISyntaxException
	 * @throws ORESerialiserException
	 */
	private CountingInputStream getOREDocContents(String ORE, String SMO, String SDO) 
			throws OREException, URISyntaxException, ORESerialiserException {
		Identifier smoId = new Identifier();
		smoId.setValue(SMO);

		List<Identifier> dataObjects = new Vector<Identifier>();
		Identifier sdoId = new Identifier();
		sdoId.setValue(SDO);
		dataObjects.add(sdoId);
		
		Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
		idMap.put(smoId, dataObjects);
		
		Identifier oreId = new Identifier();
		oreId.setValue(ORE);
		
		ResourceMap rm = ResourceMapFactory.getInstance().createResourceMap(oreId, idMap);
		String resourceMapXML = ResourceMapFactory.getInstance().serializeResourceMap(rm);
		return new CountingInputStream(new ByteArrayInputStream(resourceMapXML.getBytes()));
	}
		
	/**
	 * This takes a DAP URL and makes a DAP/D1 Servlet PID from it. The PID is a unique
	 * reference to the DAP URL. On minor point is that the 'http://' prefix is stripped
	 * off the front of the URL if it is present because these PIDs will need to be passed
	 * into the servlet as part of the URL path (e.g., http://MACH/d1/mn/object/PID) and
	 * tomcat (and others?) will convert '//' to '/' in the path part of the URL they 
	 * receive. So, to avoid confusion and excess work in the prototype, we're going to 
	 * remove it now. This means only HTTP URLs will work with the prototype servlet.
	 * 
	 * TODO Fix this code (and/or the database) so that https DAP URLs work too.
	 * 
	 * @param URL
	 * @param kind
	 * @param serialNumber
	 * @return
	 * @throws Exception
	 */
	private String buildId(String URL, String kind, Long serialNumber) throws Exception {
		// parse the URL: http://<mach&port>/<path> so we can make it look like
		// http://<mach&port>/dataone_<kind>_<serial_no>/<path>
		
		if (URL.indexOf("https://") == 0)
			throw new Exception("Malformed URL (cannot use HTTPS URLs.");
		
		if (URL.indexOf("http://") == 0)
			URL = URL.substring(7);

		int startOfPath = URL.indexOf('/'); // 'path' will include the '/' character
		if (startOfPath < 0)
			throw new Exception("Malformed URL (could not find path separator '/'.");
		String path = URL.substring(startOfPath);
		String host = URL.substring(0, startOfPath);
		
		return host + "/dataone_" + kind + "_" + serialNumber.toString() + path;
	}
	
	private String buildDAPURL(String URL, String extension) {
		return URL + extension;
	}
	
	private String ISO8601(Date time) {
		return String.format("%tFT%<tRZ", time);
	}

	/**
	 * Dump the database contents to stdout. This only dumps the fields common
	 * to all of the PIDs in the database (it does not show the DAP URL or 
	 * ORE document info). This is a debugging and diagnostic tool.
	 * 
	 * @throws SQLException
	 */
	public void dump() throws SQLException {
		Statement stmt = c.createStatement();
		ResultSet rs = null;
		try {
			String sql = "SELECT * FROM Metadata ORDER BY ROWID;";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("Id = " + rs.getString("Id"));
				System.out.println("Date = " + rs.getString("dateAdded"));
				System.out.println("FormatId = " + rs.getString("format"));
				System.out.println("Size = " + rs.getString("size"));
				System.out.println("Checksum = " + rs.getString("checksum"));
				System.out.println("Algorithm = " + rs.getString("algorithm"));
				System.out.println();
			}

		} catch (SQLException e) {
			log.error("Failed to dump database tables (" + dbName + ").");
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}
	
	/**
	 * How many PIDs are in the database for the DAP D1 servlet. This includes
	 * PIDs that are obsolete (when support for those has been added to the database).
	 *
	 * @return The count of Unique PIDs.
	 * @throws SQLException
	 */
	public int count() throws SQLException {
		Statement stmt = c.createStatement();
		ResultSet rs = null;
		try {
			String sql = "SELECT COUNT(*) FROM Metadata ORDER BY ROWID;";

			int rows = 0;
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				rows = rs.getInt("COUNT(*)");
			}

			return rows;
			
		} catch (SQLException e) {
			log.error("Failed to dump database tables (" + dbName + ").");
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}
	
	/**
	 * For a given D1 PID, return its FormatId. For the DAP D1 servlet, this will be
	 * one of "netcdf", "iso19115" or the long ORE namespace URI. In the future the 
	 * DAP D1 servlet might support more formats.
	 * 
	 * @param pid The D1 PID
	 * @return The format as a string
	 * @throws SQLException
	 * @throws Exception
	 */
	public String getFormatId(String pid) throws SQLException, DAPDatabaseException {
		return getTextMetadataItem(pid, "format");
	}
	
	public Date getDateSysmetaModified(String pid) throws SQLException, DAPDatabaseException {
		String dateString =  getTextMetadataItem(pid, "dateAdded");
		try {
			return DateUtils.parseDate(dateString, new String[]{"yyyy-MM-dd'T'HH:mm'Z'"});
		} catch (ParseException e) {
			throw new DAPDatabaseException("Corrupt database. Malformed date/time for '" + pid + "': " + e.getMessage());	
		}
	}

	/**
	 * Return the size of the object.
	 * 
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public String getSize(String pid) throws SQLException, DAPDatabaseException {
		return getTextMetadataItem(pid, "size");
	}

	/**
	 * Return the serialNumber of the object.
	 * 
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public BigInteger getSerialNumber(String pid) throws SQLException, DAPDatabaseException {
		return new BigInteger(getTextMetadataItem(pid, "serialNumber"));
	}
	
	/**
	 * Return the checksum for the object.
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public String getChecksum(String pid) throws SQLException, DAPDatabaseException {
		return getTextMetadataItem(pid, "checksum");
	}
	
	/**
	 * Return the checksum algorithm for the object.
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public String getAlgorithm(String pid) throws SQLException, DAPDatabaseException {
		return getTextMetadataItem(pid, "algorithm");
	}
	
	private String getTextMetadataItem(String pid, String field) throws SQLException, DAPDatabaseException {
		Statement stmt = c.createStatement();
		ResultSet rs = null;
		String item = null;
		try {
			int count = 0;
			
			String sql = "SELECT " + field + " FROM Metadata WHERE Id = '" + pid + "';";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				count++;
				item = rs.getString(field);
			}

			switch (count) {
			case 0:
				throw new DAPDatabaseException("Corrupt database. Did not find '" + field + "' for '" + pid + "'.");

			case 1:
				return item;

			default:
				throw new DAPDatabaseException("Corrupt database. Found more than one '" + field + "' for '" + pid + "'.");
			}
			
		} catch (SQLException e) {
			log.error("Corrupt database (" + dbName + ").");
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}
	

	/**
	 * Does the D1 PID reference a DAP URL? This uses a pretty weak test - it assumes
	 * that there is one format for an ORE document and that if a PID does not reference
	 * one of those, it must be a DAP URL.
	 * 
	 * @param pid The D1 PID
	 * @return True if it does.
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public boolean isDAPURL(String pid) throws SQLException, DAPDatabaseException {
		try {
			return !getFormatId(pid).equals(DatasetsDatabase.ORE_FORMAT);
		} catch (SQLException e) {
			log.error("Corrupt database (" + dbName + ").");
			throw e;
		} catch (DAPDatabaseException e) {
			log.error("Corrupt database (" + dbName + ").");
			throw e;
		}
	}
	
	/** 
	 * Given that the D1 PID references a DAP URL, get the URL. This method will return
	 * either a SMO or SDO URL, but you can't tell which kind. Dereferencing the DAP URL
	 * returns the object (either a netcdf file or ISO 19111 document for now - 6/4/14).
	 * 
	 * @param pid The D1 PID
	 * @return The DAP URL
	 * @throws SQLException
	 * @throws DAPDatabaseException If there's an issue with the database or
	 * if the PID does not actually reference a DAP URL.
	 */
	public String getDAPURL(String pid) throws SQLException, DAPDatabaseException {
		Statement stmt = c.createStatement();
		ResultSet rs = null;
		try {
			String URL = null;
			int count = 0;
			String sql = "SELECT SDO.DAP_URL FROM SDO WHERE SDO.Id = '" + pid + "';";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				count++;
				URL = rs.getString("DAP_URL");
			}

			sql = "SELECT SMO.DAP_URL FROM SMO WHERE SMO.Id = '" + pid + "';";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				count++;
				URL = rs.getString("DAP_URL");
			}
			
			switch (count) {
			case 0:
				throw new DAPDatabaseException("Did not find a DAP URL entry for '" + pid + "'.");

			case 1:
				return URL;

			default:
				throw new DAPDatabaseException("Corrupt database. Found more that one entry for '" + pid + "'.");	
			}
		} catch (SQLException e) {
			log.error("Corrupt database (" + dbName + "): " + e.getMessage());
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}
	
	/**
	 * Look in the database and find the SMO and SDO that are paired. Return the 
	 * two PIDs in a Vector - an enhancement later might has the first element be
	 * a SMO and the remaining N-1 elements be the SDOs in an aggregation. For 
	 * the current version, anything other than a single pair of DAP D1 PIDs is
	 * an error.
	 * 
	 * @param pid The ORE Identifier
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public List<String> getIdentifiersForORE(String pid) throws SQLException, DAPDatabaseException {
		Statement stmt = c.createStatement();
		ResultSet rs = null;
		try {
			Vector<String> ids = new Vector<String>();
			String sql = "SELECT ORE.SDO_Id, ORE.SMO_Id FROM ORE WHERE ORE.Id = '" + pid + "';";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				ids.add(rs.getString("SMO_Id"));
				ids.add(rs.getString("SDO_Id"));
			}
			
			switch (ids.size()) {
			case 0:
			case 1:
				throw new DAPDatabaseException("Did not find the identifiers for '" + pid + "'.");

			case 2:
				return ids;

			default:
				throw new DAPDatabaseException("Corrupt database. Found more that one pair of ids for '" + pid + "'.");	
			}
		} catch (SQLException e) {
			log.error("Corrupt database (" + dbName + "): " + e.getMessage());
			throw e;
		} finally {
			rs.close();
			stmt.close();
		}
	}

}
