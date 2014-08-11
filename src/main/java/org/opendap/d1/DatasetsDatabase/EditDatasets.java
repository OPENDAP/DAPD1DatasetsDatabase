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
	
	//private static boolean warnings = false;
	
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
		options.addOption("d", "dump", false, "Dump tables from database");
		
		options.addOption("r", "read", true, "Read dataset URLs from a file, else read URLs from the command line");
		
		options.addOption("a", "add", true, "Add the dataset(s) either from the command line or a file");
		options.addOption("u", "update", true, "If this URL, or any URL in the file of URLs, is already in the DB, update it's metadata");
		options.addOption("o", "obsoletes", true, "Used with -u, this URL is obsoleted by the other URL (-u U_new -o U_obsoleted");
		
		// options.addOption("w", "warn", true, "If there are errors because a URL is already in the DB, make them errors and keep going");
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
		    
		    //boolean warnings = line.hasOption("w");
		    
		    String remainingArgs[] = line.getArgs();
		    if (remainingArgs.length < 1) {
		    	System.err.println("Expected the database name.");
		    	return;
		    }
		    
		    String dbName = remainingArgs[0];
		    if (verbose) {
		    	ps.println("Database Name: " + dbName);
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

		    // Read URLs to datasets from a file or stdin
		    if (line.hasOption("r")) {
		    	BufferedReader br = null;
		    	if (line.getOptionValue("r") == "-") {
		    		// Use stdin
		    		br = new BufferedReader(new InputStreamReader(System.in));
		    	}
		    	else {
		    		// Open the file
			    	br = new BufferedReader(new InputStreamReader(new FileInputStream(line.getOptionValue("r"))));
		    	}
		    	String strLine;

		    	// Read File Line By Line
		    	while ((strLine = br.readLine()) != null)   {
		    		String URLs[] = strLine.split("[ \t,]");
			    	if (verbose) {
			    		if (URLs.length > 1)
			    			ps.println("Updating URL in database: " + URLs[0] + ", replaces: " + URLs[1]);
			    		else
			    			ps.println("Adding/Updating URL in database: " + URLs[0]);
			    	}
			    	
			    	if (URLs.length > 1)
			    		db.updateDataset(URLs[0], URLs[1]);
			    	else if (db.isInDatabase(URLs[0])) 
			    		db.updateDataset(URLs[0]);
			    	else
			    		db.addNewDataset(URLs[0]);
		    	}

		    	//Close the input stream
		    	br.close();
		    }
		    // handle the 'add new' option
		    else if (line.hasOption("a")) {
		    	if (line.getOptionValue("a") != null) {
			    	if (verbose)
			    		ps.println("Adding URL to database: " + line.getOptionValue("a"));
			    	
			    	db.addNewDataset(line.getOptionValue("a"));
			    }
			    else {
			    	System.err.println("When using -a you must supply a URL.");
			    	return;
			    }
		    }
		    else if (line.hasOption("u")) { // update existing URLs
			    if (line.getOptionValue("u") != null) {
			    	if (verbose) {
			    		if (line.getOptionValue("o") != null)
			    			ps.println("Updating URL in database: " + line.getOptionValue("u") + ", replaces: " + line.getOptionValue("o"));
			    		else
			    			ps.println("Updating URL in database: " + line.getOptionValue("u"));
			    	}

			    	if (line.getOptionValue("o") != null)
			    		db.updateDataset(line.getOptionValue("u"), line.getOptionValue("o"));
			    	else
			    		db.updateDataset(line.getOptionValue("u"));
			    }
			    else {
			    	System.err.println("When using -u you must supply a URL to update (and may also supply an 'old' URL this replaces with -o).");
			    	return;
			    }
		    }
		    else if (line.hasOption("d")) { // dump
		    	db.dump();
		    }
		    else {	// error; must use -a or -u
		    	System.err.println("You must use -a (add), -u (update), -r (read from file/stdin) or -d (dump)");
		    	return;
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
