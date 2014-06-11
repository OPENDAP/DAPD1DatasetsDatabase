package org.opendap.d1.DatasetsDatabase;

import java.math.BigInteger;
import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

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
		this.dateSystemMetadataModified = DateUtils.parseDate(dateAdded, new String[]{"yyyy-MM-dd'T'HH:mm:ss"});
		// DateTimeMarshaller.deserializeDateToUTC(dateAdded);
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
