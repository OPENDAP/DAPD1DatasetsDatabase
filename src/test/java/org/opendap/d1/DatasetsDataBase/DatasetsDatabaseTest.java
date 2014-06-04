/**
 * 
 */
package org.opendap.d1.DatasetsDataBase;

import java.sql.SQLException;
import java.util.List;

import org.opendap.d1.DatasetsDatabase.DAPDatabaseException;
import org.opendap.d1.DatasetsDatabase.DatasetsDatabase;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author jimg
 *
 */
public class DatasetsDatabaseTest extends TestCase {

	DatasetsDatabase db = null;
	
	private static String fnoc1_smo = "test.opendap.org/dataone_smo_1/opendap/hyrax/data/nc/fnoc1.nc";
	private static String fnoc1_sdo = "test.opendap.org/dataone_sdo_1/opendap/hyrax/data/nc/fnoc1.nc";
	private static String fnoc1_ore = "test.opendap.org/dataone_ore_1/opendap/hyrax/data/nc/fnoc1.nc";
	
	private static String fnoc1_smo_url = "http://test.opendap.org/opendap/hyrax/data/nc/fnoc1.nc.iso";
	private static String fnoc1_sdo_url = "http://test.opendap.org/opendap/hyrax/data/nc/fnoc1.nc.nc";

	/**
     * Create the test case
     *
	 * @param name Name of the test case
	 */
	public DatasetsDatabaseTest(String name) {
		super(name);
		try {
			db = new DatasetsDatabase("test.db");
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Could not open database");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			fail("Could not open database");
		}
	}

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( DatasetsDatabaseTest.class );
    }


	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#addDataset(java.lang.String)}.
	 *
	public void testAddDataset() {
		fail("Not yet implemented");
	}
	*/
	
	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#count()}.
	 */
	public void testCount() {
		try {
			assertEquals("Count should return 6 with test.db", db.count(), 6);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		}
	}

	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#getFormatId(java.lang.String)}.
	 */
	public void testGetFormatId() {
		try {
			assertEquals("The fnoc1_smo PID should have a formatId of '" + DatasetsDatabase.SMO_FORMAT + "'",
					db.getFormatId(fnoc1_smo), DatasetsDatabase.SMO_FORMAT);
			assertEquals("The fnoc1_sdo PID should have a formatId of '" + DatasetsDatabase.SDO_FORMAT + "'",
					db.getFormatId(fnoc1_sdo), DatasetsDatabase.SDO_FORMAT);
			assertEquals("The fnoc1_ore PID should have a formatId of '" + DatasetsDatabase.ORE_FORMAT + "'",
					db.getFormatId(fnoc1_ore), DatasetsDatabase.ORE_FORMAT);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		} catch (DAPDatabaseException e) {
			e.printStackTrace();
			fail("Caught DAPDatabaseException.");
		}
	}
	
	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#isDAPURL(java.lang.String)}.
	 */
	public void testIsDAPURL() {
		try {
			assertTrue("The fnoc1_smo PID references a DAP URL", db.isDAPURL(fnoc1_smo));
			assertTrue("The fnoc1_sdo PID  references a DAP URL", db.isDAPURL(fnoc1_sdo));
			assertFalse("The fnoc1_ore PID  does not reference a DAP URL", db.isDAPURL(fnoc1_ore));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		} catch (DAPDatabaseException e) {
			e.printStackTrace();
			fail("Caught DAPDatabaseException.");
		}
	}

	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#isValid(java.lang.String)}.
	 */
	public void testIsValid() {
		try {
			DatasetsDatabase emptyDB = new DatasetsDatabase("/dev/null");;
			assertTrue("The database is not valid, but it should be.", db.isValid());
			assertFalse("The database is not valid, but it passed the validity test.", emptyDB.isValid());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			fail("Caught ClassNotFoundException");
		}
	}
	
	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#getDAPURL(java.lang.String)}.
	 */
	public void testGetDAPURL() {
		try {
			assertEquals("The DAP URL was wrong.", db.getDAPURL(fnoc1_sdo), fnoc1_sdo_url);
			assertEquals("The DAP URL was wrong.", db.getDAPURL(fnoc1_smo), fnoc1_smo_url);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		} catch (DAPDatabaseException e) {
			e.printStackTrace();
			fail("Caught DAPDatabaseException");
		}
	}

	/**
	 * Test method for {@link org.opendap.d1.DatasetsDatabase.DatasetsDatabase#getIdentifiersForORE(java.lang.String)}.
	 */
	public void testGetIdentifiersForORE() {
		try {
			List<String> ids = db.getIdentifiersForORE(fnoc1_ore);
			assertEquals("This should be the SMO PID", ids.get(0), fnoc1_smo);
			assertEquals("This should be the SDO PID", ids.get(1), fnoc1_sdo);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Caught SQLException.");
		} catch (DAPDatabaseException e) {
			e.printStackTrace();
			fail("Caught DAPDatabaseException");
		}
	}
}
