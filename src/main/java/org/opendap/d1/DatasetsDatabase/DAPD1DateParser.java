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

import java.text.ParseException;
import java.util.Date;

import org.dataone.service.util.DateTimeMarshaller;

/**
 * Centralize parsing strings to Date objects and Date objects to strings for
 * the DAP D1 server. This parses/formats Strings/Dates so that we can easily
 * work with ISO 8601 date/time strings.
 * 
 * @author James Gallagher
 *
 */
public class DAPD1DateParser {

	public DAPD1DateParser() {

	}

	public static Date StringToDate(String dateString) throws ParseException {
		return DateTimeMarshaller.deserializeDateToUTC(dateString);
		
		// This is slightly broken WRT the docs because they show the time zone
		// as 00:00 and not 0000 which is what Z will parse. 
		// http://mule1.dataone.org/ArchitectureDocs-current/apis/Types.html#Types.DateTime
		// jhrg 6/12/14
		//
		// return DateUtils.parseDate(dateString, new String[]{"yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS", 
		//		"yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"});
	}
	
	public static String DateToString(Date date) {
		return DateTimeMarshaller.serializeDateToUTC(date);
		// return String.format("%tFT%<tT", date);
	}
}
