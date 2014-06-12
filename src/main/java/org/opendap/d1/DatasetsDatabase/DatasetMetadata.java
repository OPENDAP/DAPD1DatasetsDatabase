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

import java.math.BigInteger;
import java.text.ParseException;
import java.util.Date;

public class DatasetMetadata {

	private String PID;
	private String format;
	private String checksum;
	private String algorithm;
	private BigInteger size;
	private Date dateSystemMetadataModified;
	
	public DatasetMetadata() {
	}

	/**
	 * Convenience ctor.
	 * 
	 * Note that dateAdded is a reminder that the value in the database is really the
	 * date this entry was added, which is the same as the ...modified date until we actually
	 * support modifying entries.
	 * 
	 * @param PID
	 * @param format
	 * @param checksum
	 * @param algorithm
	 * @param size
	 * @param dateAdded
	 * @throws ParseException 
	 */
	public DatasetMetadata(String PID, String format, String checksum, String algorithm, String size, 
			String dateAdded) throws ParseException {
		this.PID = PID;
		this.format = format;
		this.checksum = checksum;
		this.algorithm = algorithm;
		this.size = new BigInteger(size);
		this.dateSystemMetadataModified = DAPD1DateParser.StringToDate(dateAdded);
	}
	
	public String getPID() {
		return PID;
	}

	public void setPID(String PID) {
		this.PID = PID;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getChecksum() {
		return checksum;
	}

	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}

	public String getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}

	public BigInteger getSize() {
		return size;
	}

	public void setSize(BigInteger size) {
		this.size = size;
	}

	public Date getDateSystemMetadataModified() {
		return dateSystemMetadataModified;
	}

	public void setDateSystemMetadataModified(Date dateSystemMetadataModified) {
		this.dateSystemMetadataModified = dateSystemMetadataModified;
	}
}
