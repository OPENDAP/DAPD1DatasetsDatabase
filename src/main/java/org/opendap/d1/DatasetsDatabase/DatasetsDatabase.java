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
import java.sql.PreparedStatement;
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
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dspace.foresite.OREException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.ResourceMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public static String SMO_FORMAT = "INCITS 453-2009";	// ISO 19115 North America
	
	/// D1 ORE
	public static String ORE_IDENT = "ore";
	/// ORE D1 formatID
	public static String ORE_FORMAT = "http://www.openarchives.org/ore/terms";

	private static Logger log = LoggerFactory.getLogger(DatasetsDatabase.class);
	
	private static String dbName = "";
	private Connection c = null;
	
	/**
	 * Open a connection to the database. Note that the database is kept open until
	 * the object goes out of scope, when its finalize() method closes the connection.
	 * 
	 * @param name The name of the database file
	 * @exception Exception is thrown if the SQLite class is not found or the 
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
	 * SQLite on my computer does not seem to support them or the syntax is odd
	 * or something. See the SQLite docs at
	 * http://www.sqlite.org/foreignkeys.html for more info on this including to
	 * compile SQLite so that it supports foreign keys.
	 * 
	 * @throws SQLException Thrown if the tables already exist
	 */
	protected void initTables() throws SQLException {
		Statement stmt = c.createStatement();

		try {
			// This table holds the system metadata, except for the 'obsoletes' information,
			// for a given D1 Persistent ID.
			String sql = "CREATE TABLE Metadata "
					+ "(Id			TEXT NOT NULL," // FOREIGN KEY; sqlite might not support this
					+ " dateAdded 	TEXT NOT NULL,"
					+ " serialNumber INT NOT NULL,"
					+ " format 		TEXT NOT NULL,"
					+ " size 		TEXT NOT NULL,"
					+ " checksum 	TEXT NOT NULL,"
					+ " algorithm 	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			// This matches the PID for an ORE document with the PIDs for the SDO and SMO
			// and includes the actual ORE document. Building these on the fly did not work
			// because the checksum would never match since the documents contain timestamps
			// that mark when they are made.
			sql = "CREATE TABLE ORE "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " SMO_Id 	TEXT NOT NULL,"
					+ " SDO_Id 	TEXT NOT NULL,"
					+ " ORE_Doc	BLOB NOT NULL)";
			stmt.executeUpdate(sql);
			
			// Match the PID for a science data object with the URL that will return the SDO.
			sql = "CREATE TABLE SDO "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " DAP_URL	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			// ...ditto for the SMO
			sql = "CREATE TABLE SMO "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " DAP_URL	TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			// Record which PIDs supersede older PIDs 
			sql = "CREATE TABLE Obsoletes "
					+ "(Id		TEXT NOT NULL," // FOREIGN KEY
					+ " Previous TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
			// Record the base URL for a DAP dataset and its associated PIDs.
			sql = "CREATE TABLE Datasets "
					+ "(DAP_BASE_URL	TEXT NOT NULL," // FOREIGN KEY
					+ " SDO_Id 			TEXT NOT NULL,"
					+ " SMO_Id 			TEXT NOT NULL,"
					+ " ORE_Id			TEXT NOT NULL)";
			stmt.executeUpdate(sql);
			
		} catch (SQLException e) {
			log.error("Failed to create new database tables (" + dbName + ").");
			throw e;
		} finally {
			stmt.close();			
		}

		log.debug("Made database tables successfully (" + dbName + ").");
	}
	
	/**
	 * Is this database valid. This is a copy of the same code in LogDatabase over in d1Servlet.
	 * 
	 * I could factor this out and make the tableNames and Connection object parameters...
	 * 
	 * @return
	 * @throws SQLException
	 */
	public boolean isValid() throws SQLException {
		final Set<String> tableNames = new HashSet<String>(Arrays.asList("Metadata", "ORE", "SMO", "SDO", "Obsoletes", "Datasets"));
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = c.prepareStatement("SELECT name FROM sqlite_master WHERE type='table';");
			rs = stmt.executeQuery();
			int count = 0;
			while (rs.next()) {
				count++;
				String name = rs.getString("name");
				if (!tableNames.contains(name)) {
					log.debug("Database failed validity test; does not have table: {}", name);
					return false;
				}
			}
			if (count != tableNames.size()) {
				log.debug("Database failed validity test; does not have the required tables.");
				return false;
			}
			
			// All tests passed
			return true;
		} catch (SQLException e) {
			log.error("Error querying the log database ({}).", dbName);
			throw e;
		}
		finally {
			if (rs != null)
				rs.close();
			if (stmt != null)
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
			log.error("Error querying the database ({}) for PID: {}.", dbName, pid);
			return false;
		} catch (DAPDatabaseException e) {
			log.error("Error querying the database ({}) for PID: {}.", dbName, pid);
			return false;
		}
	}
	
	public boolean isInDatabase(String dapURL) throws SQLException {
		PreparedStatement stmt = c.prepareStatement("SELECT COUNT(*) FROM Datasets WHERE DAP_BASE_URL = ?;");
		try {
			stmt.setString(1,  dapURL);
			ResultSet rs = stmt.executeQuery();
			int count = 0;
			while(rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			
			return count == 1;
		}
		finally {
			stmt.close();
		}
	}
	
	/**
	 * This version of loadDataset assumes that the SDO will be a netCDF file
	 * and the SMO will be a ISO 19115 document. It uses the current time as
	 * 'date added or modified.'
	 * 
	 * @param dapURL This is the base URL of the DAP Access point. DAP2 assumed.
	 * @throws SQLException
	 */
	public void addNewDataset(String dapURL) throws SQLException, Exception {
		c.setAutoCommit(false);
		PreparedStatement d_stmt = c.prepareStatement("INSERT INTO Datasets (DAP_BASE_URL, SDO_ID, SMO_ID, ORE_ID) VALUES (?, ?, ?, ?);");
		try {
			Long serialNumber = new Long(1);	// when calling, dataset is always new
			insertURL(dapURL, serialNumber);
			
			// "INSERT INTO Datasets (DAP_BASE_URL, SDO_ID, SMO_ID, ORE_ID) VALUES (?, ?, ?, ?);"
			d_stmt.setString(1, dapURL);
			d_stmt.setString(2, buildId(dapURL, SDO_IDENT, serialNumber));
			d_stmt.setString(3, buildId(dapURL, SMO_IDENT, serialNumber));
			d_stmt.setString(4, buildId(dapURL, ORE_IDENT, serialNumber));
			d_stmt.executeUpdate();

		} catch (SQLException e) {
			log.error("Failed to load new dataset information in the database ({}).", dbName);
			throw e;
		} finally {
			d_stmt.close();
			c.commit();
		}
	}

	/**
	 * Update a dataset's entries. This does essentially what addNewDataset does,
	 * with the exception that it increments the serial number and puts an entries
	 * in the 'Obsoletes' table.
	 *  
	 * @param dapURL
	 * @param oldURL IF this is null, simply update the metadata for newURL.
	 * @throws SQLException
	 * @throws Exception
	 */
	public void updateDataset(String dapURL) throws SQLException, Exception {
		c.setAutoCommit(false);
		PreparedStatement o_stmt = c.prepareStatement("INSERT INTO Obsoletes (ID, Previous) VALUES (?, ?);");
		PreparedStatement d_stmt = c.prepareStatement("UPDATE Datasets SET SDO_ID = ?, SMO_ID = ?, ORE_ID = ? WHERE DAP_BASE_URL = ?;");
		
		try {
			// Get the dapURL serial number and add one to make the new serial number.
			// Assume that all PIDs for a given dataset use the same SerialNumber, so we can
			// look up the oldURL in the Datasets table, get any one of the PIDs and bump up
			// PID's serial number. 
			String SDOId = getSDOId(dapURL);

			Long newSerialNumber = getSerialNumber(SDOId).longValue() + 1;
			
			// Now update the Obsoletes table, which will have three entries (one for each
			// PID for the newURL/oldURL).
			o_stmt.setString(1, buildId(dapURL, SDO_IDENT, newSerialNumber)); // the new PID
			o_stmt.setString(2, SDOId);	// the old PID (read from the Datasets table).
			o_stmt.executeUpdate();
			
			String SMOId = getSMOId(dapURL);
			o_stmt.setString(1, buildId(dapURL, SMO_IDENT, newSerialNumber));
			o_stmt.setString(2, SMOId);
			o_stmt.executeUpdate();

			String OREId = getOREId(dapURL);
			o_stmt.setString(1, buildId(dapURL, ORE_IDENT, newSerialNumber));
			o_stmt.setString(2, OREId);
			o_stmt.executeUpdate();

			// Now update the metadata and other tables
			insertURL(dapURL, newSerialNumber);
			
			// "UPDATE Datasets SET SDO_ID = ?, SMO_ID = ?, ORE_ID = ? WHERE DAP_BASE_URL = ?;"
			d_stmt.setString(4, dapURL);
			d_stmt.setString(1, buildId(dapURL, SDO_IDENT, newSerialNumber));
			d_stmt.setString(2, buildId(dapURL, SMO_IDENT, newSerialNumber));
			d_stmt.setString(3, buildId(dapURL, ORE_IDENT, newSerialNumber));
			d_stmt.executeUpdate();

		} catch (SQLException e) {
			log.error("Failed to load updated dataset information in the database ({}).", dbName);
			throw e;
		} finally {
			o_stmt.close();
			d_stmt.close();
			c.commit();
		}
	}

	/**
	 * Even though there are two URLs, we assume they refer to the same logical dataset.
	 * @param newDapURL
	 * @param oldDapURL
	 * @throws SQLException
	 * @throws Exception
	 */
	public void updateDataset(String newDapURL, String oldDapURL) throws SQLException, Exception {
		c.setAutoCommit(false);
		PreparedStatement o_stmt = c.prepareStatement("INSERT INTO Obsoletes (ID, Previous) VALUES (?, ?);");
		PreparedStatement d_stmt = c.prepareStatement("INSERT INTO Datasets (DAP_BASE_URL, SDO_ID, SMO_ID, ORE_ID) VALUES (?, ?, ?, ?);");
		
		try {
			// Get the oldDapURL serial number and add one to make the new serial number.
			// Assume that all PIDs for a given dataset use the same SerialNumber, so we can
			// look up the oldDapURL in the Datasets table, get any one of the PIDs and bump up
			// PID's serial number, to make the new serial number.
			String oldSDOId = getSDOId(oldDapURL);

			Long newSerialNumber = getSerialNumber(oldSDOId).longValue() + 1;
			
			// Now update the Obsoletes table, which will have three entries (one for each
			// PID for the newURL/oldURL).
			o_stmt.setString(1, buildId(newDapURL, SDO_IDENT, newSerialNumber)); // the new PID
			o_stmt.setString(2, oldSDOId);	// the old PID (read from the Datasets table).
			o_stmt.executeUpdate();
			
			String oldSMOId = getSMOId(oldDapURL);
			o_stmt.setString(1, buildId(newDapURL, SMO_IDENT, newSerialNumber));
			o_stmt.setString(2, oldSMOId);
			o_stmt.executeUpdate();

			String oldOREId = getOREId(oldDapURL);
			o_stmt.setString(1, buildId(newDapURL, ORE_IDENT, newSerialNumber));
			o_stmt.setString(2, oldOREId);
			o_stmt.executeUpdate();

			// Now update the metadata and other tables
			insertURL(newDapURL, newSerialNumber);
			
			// "INSERT INTO Datasets (DAP_BASE_URL, SDO_ID, SMO_ID, ORE_ID) VALUES (?, ?, ?, ?);"
			d_stmt.setString(1, newDapURL);
			d_stmt.setString(2, buildId(newDapURL, SDO_IDENT, newSerialNumber));
			d_stmt.setString(3, buildId(newDapURL, SMO_IDENT, newSerialNumber));
			d_stmt.setString(4, buildId(newDapURL, ORE_IDENT, newSerialNumber));
			d_stmt.executeUpdate();
		} catch (SQLException e) {
			log.error("Failed to load updated dataset information in the database ({}).", dbName);
			throw e;
		} finally {
			o_stmt.close();
			d_stmt.close();
			c.commit();
		}
	}

	/**
	 * Get the SDO Id bound to the DAP Base URL 'DAPUrl'
	 * @param dapURL
	 * @param d_stmt
	 * @return
	 * @throws DAPDatabaseException, SQLException 
	 */
	private String getSDOId(String dapURL) throws DAPDatabaseException, SQLException {
		PreparedStatement d_stmt = c.prepareStatement("SELECT SDO_Id FROM Datasets WHERE DAP_BASE_URL = ?;");
		try {
			String pid = null;
			d_stmt.setString(1, dapURL);
			ResultSet rs = d_stmt.executeQuery();
			while (rs.next())
				pid = rs.getString("SDO_Id");
			rs.close();
			
			if (pid == null)
				throw new DAPDatabaseException( "The URL '" + dapURL + "' was not in the database");

			return pid;
		} finally {
			d_stmt.close();
		}
	}

	private String getSMOId(String dapURL) throws DAPDatabaseException, SQLException {
		PreparedStatement d_stmt = c.prepareStatement("SELECT SMO_Id FROM Datasets WHERE DAP_BASE_URL = ?;");
		try {
			String pid = null;
			d_stmt.setString(1, dapURL);
			ResultSet rs = d_stmt.executeQuery();
			while (rs.next())
				pid = rs.getString("SMO_Id");
			rs.close();
			
			if (pid == null)
				throw new DAPDatabaseException( "The URL '" + dapURL + "' was not in the database");

			return pid;
		} finally {
			d_stmt.close();
		}
	}

	private String getOREId(String dapURL) throws DAPDatabaseException, SQLException {
		PreparedStatement d_stmt = c.prepareStatement("SELECT ORE_Id FROM Datasets WHERE DAP_BASE_URL = ?;");
		try {
			String pid = null;
			d_stmt.setString(1, dapURL);
			ResultSet rs = d_stmt.executeQuery();
			while (rs.next())
				pid = rs.getString("ORE_Id");
			rs.close();
			
			if (pid == null)
				throw new DAPDatabaseException( "The URL '" + dapURL + "' was not in the database");

			return pid;
		} finally {
			d_stmt.close();
		}
	}

	/**
	 * This is used by updateDataset and addNewDataset. The caller must call commit() on
	 * the open DB connection.
	 * 
	 * @param dapURL The URL to add
	 * @param serialNumber It's serial number
	 * @return
	 * @throws Exception
	 * @throws SQLException
	 */
	private void insertURL(String dapURL, Long serialNumber) throws SQLException, Exception {
		PreparedStatement m_stmt = c.prepareStatement("INSERT INTO Metadata (Id,dateAdded,serialNumber,format,size,checksum,algorithm) VALUES (?, ?, ?, ?, ?, ?, ?);");
		PreparedStatement sdo_stmt = c.prepareStatement("INSERT INTO SDO (Id, DAP_URL) VALUES (?,?);");
		PreparedStatement smo_stmt = c.prepareStatement("INSERT INTO SMO (Id, DAP_URL) VALUES (?,?);");
		PreparedStatement ore_stmt = c.prepareStatement("INSERT INTO ORE (Id, SDO_Id, SMO_Id, ORE_Doc) VALUES (?, ?, ?, ?);");
		
		// Use this ISO601 time string for all three entries
		String now8601 = DAPD1DateParser.DateToString(new Date());

		try {
			// First add the SDO info
			String sdoId = buildId(dapURL, SDO_IDENT, serialNumber); // reuse SDO
			String sdoUrl = buildDAPURL(dapURL, SDO_EXT);

			// "INSERT INTO SDO (Id, DAP_URL) VALUES (?,?);"
			sdo_stmt.setString(1, sdoId);
			sdo_stmt.setString(2, sdoUrl);
			sdo_stmt.executeUpdate();

			CountingInputStream cis = getDAPURLContents(sdoUrl);
			Checksum checksum = ChecksumUtil.checksum(cis, "SHA-1");
			Long size = new Long(cis.getByteCount());
			cis.close();

			insertMetadata(m_stmt, now8601, serialNumber, sdoId, SDO_FORMAT, checksum, size);

			// Then add the SMO info
			String smoId = buildId(dapURL, SMO_IDENT, serialNumber);
			String smoUrl = buildDAPURL(dapURL, SMO_EXT);

			// "INSERT INTO SMO (Id, DAP_URL) VALUES (?,?);"
			smo_stmt.setString(1, smoId);
			smo_stmt.setString(2, smoUrl);
			smo_stmt.executeUpdate();

			cis = getDAPURLContents(smoUrl);
			checksum = ChecksumUtil.checksum(cis, "SHA-1");
			size = new Long(cis.getByteCount());
			cis.close();

			insertMetadata(m_stmt, now8601, serialNumber, smoId, SMO_FORMAT, checksum, size);

			// Then add the ORE info
			String oreId = buildId(dapURL, ORE_IDENT, serialNumber);

			String resourceMapXML = getOREDoc(oreId, smoId, sdoId);
			cis = new CountingInputStream(new ByteArrayInputStream(resourceMapXML.getBytes()));
			checksum = ChecksumUtil.checksum(cis, "SHA-1");
			size = new Long(cis.getByteCount());
			cis.close();

			// "INSERT INTO ORE (Id, SDO_Id, SMO_Id, ORE_Doc) VALUES (?, ?, ?, ?);"
			ore_stmt.setString(1, oreId);
			ore_stmt.setString(2, sdoId);
			ore_stmt.setString(3, smoId);
			ore_stmt.setBytes(4, resourceMapXML.getBytes());
			ore_stmt.executeUpdate();

			insertMetadata(m_stmt, now8601, serialNumber, oreId, ORE_FORMAT, checksum, size);
			
			
		} catch (SQLException e) {
			log.error("Failed to insert URL into database ({}).", dbName);
			throw e;
		} finally {
			sdo_stmt.close();
			smo_stmt.close();
			ore_stmt.close();

			m_stmt.close();
		}
	}

	/**
	 * Convenience method to set columns in the creatively-named 'Metadata' table.
	 * 
	 * @param now8601
	 * @param servialNumber
	 * @param PID
	 * @param format
	 * @param checksum
	 * @param size
	 * @throws SQLException
	 */
	private void insertMetadata(PreparedStatement stmt, String now8601,
			Long serialNumber, String PID, String format, Checksum checksum,
			Long size) throws SQLException {
		// "INSERT INTO Metadata (Id,dateAdded,serialNumber,format,size,checksum,algorithm) VALUES (?, ?, ?, ?, ?, ?, ?);");
		stmt.setString(1, PID);
		stmt.setString(2, now8601);
		stmt.setString(3, serialNumber.toString());
		stmt.setString(4, format);
		stmt.setString(5, size.toString());
		stmt.setString(6, checksum.getValue());
		stmt.setString(7, checksum.getAlgorithm());

		stmt.executeUpdate();
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
	 * Build the ORE document using the SDO and SMO PIDs
	 * 
	 * @param ORE A String that holds the D1 PID for the ORE document 
	 * @param SMO A String that holds the D1 PID for the SMO document
	 * @param SDO A String that holds the D1 PID for the SDO document
	 * @return An instance of CountingInputStream
	 * @throws OREException
	 * @throws URISyntaxException
	 * @throws ORESerialiserException
	 */
	private String getOREDoc(String ORE, String SMO, String SDO) 
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
		return resourceMapXML;
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
			System.out.println("Metadata:");
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

			sql = "SELECT * FROM Datasets ORDER BY ROWID;";
			System.out.println("Datasets:");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("Id = " + rs.getString("DAP_BASE_URL"));
				System.out.println("SDO ID = " + rs.getString("SDO_ID"));
				System.out.println("SMO ID = " + rs.getString("SMO_ID"));
				System.out.println("ORE ID = " + rs.getString("ORE_ID"));
				System.out.println();
			}

			sql = "SELECT * FROM SDO ORDER BY ROWID;";
			System.out.println("SDO:");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("Id = " + rs.getString("id"));
				System.out.println("DAP_URL = " + rs.getString("DAP_URL"));
				System.out.println();
			}

			sql = "SELECT * FROM SMO ORDER BY ROWID;";
			System.out.println("SMO:");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("Id = " + rs.getString("id"));
				System.out.println("DAP_URL = " + rs.getString("DAP_URL"));
				System.out.println();
			}


			sql = "SELECT * FROM ORE ORDER BY ROWID;";
			System.out.println("ORE:");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("Id = " + rs.getString("id"));
				System.out.println("SDO_Id = " + rs.getString("SDO_Id"));
				System.out.println("SMO_Id = " + rs.getString("SMO_Id"));
				System.out.println();
			}

			sql = "SELECT * FROM Obsoletes ORDER BY ROWID;";
			System.out.println("Obsoletes:");
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println("ID = " + rs.getString("ID"));
				System.out.println("Previous = " + rs.getString("Previous"));
				System.out.println();
			}

		} catch (SQLException e) {
			log.error("Failed to dump database tables (" + dbName + ").");
			throw e;
		} finally {
			if (rs != null)
				rs.close();
			stmt.close();
		}
	}
	
	/**
	 * Build the text of a where clause that can be passed to prepareStatement and then
	 * populated with values using populateMetadataWhereClause(). This is used by the 
	 * '/object' (or listObjects()) call to selectively list PIDs stored in the database.
	 *  
	 * @param baseSQL
	 * @param fromDate
	 * @param toDate
	 * @param format
	 * @param suffix
	 * @return
	 */
	private String buildMetadataWhereClause(String baseSQL, Date fromDate, Date toDate, ObjectFormatIdentifier format, String suffix) {
		if (fromDate != null || toDate != null || format != null) {
			baseSQL += " where";
			String and = "";
			if (fromDate != null) {
				baseSQL += " dateAdded >= ?";
				and = " and";
			}
			if (toDate != null) {
				baseSQL += and + " dateAdded < ?";
				and = " and";
			}
			if (format != null) {
				baseSQL += and + " format = ?";
			}
		}
		
		return baseSQL + suffix;
	}
	
	/**
	 * This depends on the PreparedStatement being built using buildMetadataWhereClause().
	 * @param fromDate
	 * @param toDate
	 * @param format
	 * @param stmt
	 * @throws SQLException
	 */
	private void populateMetadataWhereClause(Date fromDate, Date toDate, ObjectFormatIdentifier format, PreparedStatement stmt)
			throws SQLException {
		int position = 1; // SQL uses ones-indexing
		if (fromDate != null)
			stmt.setString(position++, DAPD1DateParser.DateToString(fromDate));
		if (toDate != null)
			stmt.setString(position++, DAPD1DateParser.DateToString(toDate));
		if (format != null)
			stmt.setString(position, format.getValue());
	}


	/**
	 * How many PIDs are in the database for the DAP D1 servlet. This includes
	 * PIDs that are obsolete (when support for those has been added to the database).
	 * The count is subject to various query parameters.
	 *
	 * @return The count of Unique PIDs.
	 * @throws SQLException
	 */
	public int count(Date fromDate, Date toDate, ObjectFormatIdentifier format) throws SQLException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String sql = buildMetadataWhereClause("SELECT COUNT(*) FROM Metadata", fromDate, toDate, format, ";");
			log.debug("Metadata count stmt: {}", sql);
			stmt = c.prepareStatement(sql);
			
			populateMetadataWhereClause(fromDate, toDate, format, stmt);
			
			rs = stmt.executeQuery();
			
			int rows = 0;
			while (rs.next()) {
				rows = rs.getInt(1);
			}

			return rows;
			
		} catch (SQLException e) {
			log.error("Failed to count the Metadata table (" + dbName + "): " + e.getMessage());
			throw e;
		} finally {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
		}
	}
	
	/**
	 * Get the Previous PID that this PID has made obsolete.
	 * 
	 * @param PID The new PID
	 * @return THe old PID
	 * @throws SQLException
	 */
	public String getObsoletes(String PID) throws SQLException {
		String pid = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = c.prepareStatement("SELECT Previous From Obsoletes where ID = ?;");
			stmt.setString(1,  PID);
			
			rs = stmt.executeQuery();
			
			while (rs.next()) {
				pid = rs.getString(1);
			}

			return pid;
		}
		catch (SQLException e) {
			log.error("Could not query the 'Obsoletes' table for the new PID '{}' in {}", PID, dbName);
			throw e;
		}
		finally {
			if (stmt != null)
				stmt.close();
			if (rs != null)
				rs.close();
		}
	}
	
	/** 
	 * This PID was made obsolete by which PID?
	 * @param PID The obsolete PID
	 * @return The PID that made this PID obsolete
	 * @throws SQLException
	 */
	public String getObsoletedBy(String PID) throws SQLException {
		PreparedStatement stmt = c.prepareStatement("SELECT ID From Obsoletes where Previous = ?;");;
		
		try {
			String pid = null;
			stmt.setString(1,  PID);
			ResultSet rs = stmt.executeQuery();
			while (rs.next())
				pid = rs.getString(1);
			rs.close();

			return pid;
		}
		catch (SQLException e) {
			log.error("Could not query the 'Obsoletes' table for the old PID '{}' in {}", PID, dbName);
			throw e;
		}
		finally {
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
	
	/**
	 * For a given D1 PID, where was the system metadata modified?
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public Date getDateSysmetaModified(String pid) throws SQLException, DAPDatabaseException {
		String dateString =  getTextMetadataItem(pid, "dateAdded");
		try {
			return DAPD1DateParser.StringToDate(dateString);
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
		PreparedStatement stmt = c.prepareStatement("SELECT " + field + " FROM Metadata WHERE Id = ?;");

		try {
			int count = 0;
			String item = null;
			
			stmt.setString(1, pid);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				count++;
				item = rs.getString(field);
			}
			rs.close();
			
			switch (count) {
			case 0:
				throw new DAPDatabaseException("Corrupt database. Did not find '" + field + "' for '" + pid + "'.");

			case 1:
				return item;

			default:
				throw new DAPDatabaseException("Corrupt database. Found more than one '" + field + "' for '" + pid + "'.");
			}
			
		} catch (SQLException e) {
			log.error("Corrupt database ({})", dbName);
			throw e;
		} finally {
			stmt.close();
		}
	}
	
	// TODO add information about Obsoletes?
	/**
	 * Get metadata about PIDs known to this server. This method will return 'count'
	 * rows, starting at 'start'.
	 * 
	 * @param fromDate
	 * @param toDate
	 * @param format
	 * @param start
	 * @param count
	 * @return A List of DatasetMetadata objects.
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public List<DatasetMetadata> getAllMetadata(Date fromDate, Date toDate, ObjectFormatIdentifier format,
			int start, int count) throws SQLException, DAPDatabaseException {
		
		PreparedStatement stmt = null;
		Vector<DatasetMetadata> dmv = new Vector<DatasetMetadata>();
		try {
			String sql = buildMetadataWhereClause("SELECT * FROM Metadata", fromDate, toDate, format, " ORDER BY ROWID;");
			log.debug("Metadata access stmt: {}", sql);
			stmt = c.prepareStatement(sql);
			
			populateMetadataWhereClause(fromDate, toDate, format, stmt);
			
			ResultSet rs = stmt.executeQuery();

			while (start-- > 0 && rs.next());

			int lines = 0;
			while (lines < count && rs.next()) {
				++lines;
				dmv.add(new DatasetMetadata(rs.getString("Id"), rs.getString("format"), rs.getString("checksum"),
						rs.getString("algorithm"), rs.getString("size"), rs.getString("dateAdded")));
			}
			rs.close();
			
			return dmv;
		} catch (SQLException e) {
			log.error("Corrupt database (" + dbName + "): " + e.getMessage());
			throw e;
		} catch (ParseException e) {
			log.error("Corrupt database (" + dbName + "). Could not parse a Date/Time value: " + e.getMessage());
			throw new DAPDatabaseException(e.getMessage());
		} finally {
			if (stmt != null)
				stmt.close();
		}
	}
	

	/**
	 * Does the D1 PID reference a DAP URL? This uses a pretty weak test - it assumes
	 * that there is one format for an ORE document and that if a PID does not reference
	 * one of those, it must be a DAP URL. That is, that the PID references a URL that
	 * can be dereferenced and that doing so accesses a DAP server to get either a 
	 * metadata document or a dataset.
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
		PreparedStatement stmt = null;
		try {
			String URL = null;
			int count = 0;
			stmt = c.prepareStatement("SELECT SDO.DAP_URL FROM SDO WHERE SDO.Id = ?;");
			stmt.setString(1, pid);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				count++;
				URL = rs.getString("DAP_URL");
			}

			stmt = c.prepareStatement("SELECT SMO.DAP_URL FROM SMO WHERE SMO.Id = ?;");
			stmt.setString(1, pid);
			rs = stmt.executeQuery();
			while (rs.next()) {
				count++;
				URL = rs.getString("DAP_URL");
			}
			rs.close();
			
			switch (count) {
			case 0:
				throw new DAPDatabaseException("Did not find a DAP URL entry for '" + pid + "'.");

			case 1:
				return URL;

			default:
				throw new DAPDatabaseException("Corrupt database. Found more that one entry for '" + pid + "'.");	
			}
		} catch (SQLException e) {
			log.error("Corrupt database ({}): {}", dbName, e.getMessage());
			throw e;
		} finally {
			if (stmt != null)
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
		// Statement stmt = c.createStatement();
		PreparedStatement stmt = null;
		try {
			Vector<String> ids = new Vector<String>();
			//String sql = "SELECT ORE.SDO_Id, ORE.SMO_Id FROM ORE WHERE ORE.Id = '" + pid + "';";
			stmt = c.prepareStatement("SELECT ORE.SDO_Id, ORE.SMO_Id FROM ORE WHERE ORE.Id = ?;");
			stmt.setString(1, pid);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ids.add(rs.getString("SMO_Id"));
				ids.add(rs.getString("SDO_Id"));
			}
			rs.close();
			
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
			if (stmt != null)
				stmt.close();
		}
	}

	/**
	 * I added ORE Documents to the server's database because it seems that generated
	 * ORE documents have a timestamp that is set to the time they were generated, so
	 * virtually no two ORE docs are equal - they yield different checksums.
	 * 
	 * The SDO and SMO PIDs are still stored in the DB, however, so it's still possible
	 * to call getIdentifiersForORE().
	 * 
	 * @param pid
	 * @return
	 * @throws SQLException
	 * @throws DAPDatabaseException
	 */
	public String getOREDoc(String pid) throws SQLException, DAPDatabaseException {
		PreparedStatement stmt = c.prepareStatement("SELECT ORE.ORE_Doc FROM ORE WHERE ORE.Id = ?;");
		try {
			String ore_doc = null;
			stmt.setString(1, pid);
			ResultSet rs = stmt.executeQuery();
			while (rs.next())
				ore_doc = rs.getString("ORE_Doc");
			rs.close();

			if (ore_doc == null)
				throw new DAPDatabaseException("Did not find the ORE document for '" + pid + "'.");

			return ore_doc;
		} catch (SQLException e) {
			log.error("Corrupt database ({}): {}", dbName, e.getMessage());
			throw e;
		} finally {
			stmt.close();
		}
	}

}
