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

/**
 * When there's a logical error in/with the database that is not an SQLException,
 * DatasetsDatabase will throw this. An example is when the code queries the DB
 * expecting to get zero of one response and gets two or more. 
 * 
 * @author James Gallagher
 *
 */
public class DAPDatabaseException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DAPDatabaseException() {
		super("Unknown DAP D1 Servlet database error.");
	}

	public DAPDatabaseException(String message) {
		super(message);
	}

	public DAPDatabaseException(Throwable cause) {
		super(cause);
	}

	public DAPDatabaseException(String message, Throwable cause) {
		super(message, cause);
	}

}
