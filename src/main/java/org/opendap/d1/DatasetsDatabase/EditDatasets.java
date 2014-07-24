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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @brief Create, Read, Update, Delete tool for the DAP/D1 servlet's database
 * 
 * This command line tool performs the basic CRUD operations on the database used
 * by the DAP/D1 servlet. See README.dataset.db for info about the database design.
 * In the current incarnation, the database uses SQLite.
 * 
 * @author James Gallagher
 *
 */
public class EditDatasets {
	
	private static Logger log = LoggerFactory.getLogger(EditDatasets.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*
		Reader r = new InputStreamReader(EditDatasets.class.getClassLoader().getResourceAsStream("logback.xml"));
		StringWriter sw = new StringWriter();
		char[] buffer = new char[1024];
		try {
			for (int n; (n = r.read(buffer)) != -1; )
			    sw.write(buffer, 0, n);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String str = sw.toString();
		System.out.println(str);
		*/
		/*
	    // assume SLF4J is bound to logback in the current environment
	    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
	    // print logback's internal status
	    StatusPrinter.print(lc);
		 */
		final CommandLineParser parser = new GnuParser();

		Options options = new Options();

		options.addOption("v", "verbose", false, "Write info to stdout");
		options.addOption("i", "initialize", false, "Create tables for a blank database");
		options.addOption("r", "read", true, "Read dataset URLs from a file, else read URLs from the command line");
		
		options.addOption("u", "update", true, "If this URL, or any URL in the file of URLs, already is in the DB, update it's metadata");
		options.addOption("o", "obsoleted", true, "Used with -u, this URL is obsoleted by the other URL (-u U_new -o U_obsoleted");
		
		options.addOption("w", "warn", true, "If there are errors because a URL is already in the DB, make them errors and keep going");
		options.addOption("h", "help", false, "Usage information");
		
		log.debug("Starting debug logging");
		
		try {
		    CommandLine line = parser.parse( options, args );

			if (line.hasOption("h")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("EditDatasets [options] <database name> [<DAP URL>]", options);
				return;
			}

		    boolean verbose = line.hasOption("v");
		    PrintStream ps = System.out;
		    
		    String remainingArgs[] = line.getArgs();
		    if (remainingArgs.length < 1)
		    	throw new Exception("Expected the database name.");
		    
		    String dbName = remainingArgs[0];
		    if (verbose) {
		    	ps.println("Databae Name: " + dbName);
		    }
		    
		    DatasetsDatabase db = new DatasetsDatabase(dbName);
		    // Test if 'dbName' exists 
		    if (line.hasOption("i")) {
		    	if (verbose)
		    		ps.println("Initializing database...");
		    	db.initTables();
		    }
		    
		    if (!db.isValid()) {
		    	ps.println("Database opened but is not valid.");
		    	return;
		    }
		    	
		    if (line.hasOption("r")) {
		    	// Open the file
		    	FileInputStream fstream = new FileInputStream(line.getOptionValue("r"));
		    	BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		    	String strLine;

		    	//Read File Line By Line
		    	while ((strLine = br.readLine()) != null)   {
			    	if (verbose)
			    		ps.println("Adding URL to database... " + strLine);
			    	db.addDataset(strLine);
		    	}

		    	//Close the input stream
		    	br.close();
		    }
		    
		    for (int i = 1; i < remainingArgs.length; ++i) {
		    	if (verbose)
		    		ps.println("Adding URL to database... " + remainingArgs[i]);
		    	db.addDataset(remainingArgs[i]);
		    }
		    
		    if (verbose) {
		    	ps.println("Rows in the database: " + db.count(null, null, null));
		    	db.dump();
		    }
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getLocalizedMessage());
			e.printStackTrace();
			return;
		}
	}
	/*
	private static boolean doesNotExist(String name) {
		File f = new File(name);
		return !f.exists();
	}
	*/
}
