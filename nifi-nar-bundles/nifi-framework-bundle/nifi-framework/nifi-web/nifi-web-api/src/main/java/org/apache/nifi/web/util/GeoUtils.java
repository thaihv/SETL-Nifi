/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.util;

import java.awt.AlphaComposite;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import javax.imageio.ImageIO;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.api.request.LongParameter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class GeoUtils {

	private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(GeoUtils.class));

	public static byte[] createBlankTiles(int w, int h, String displayText) {
		BufferedImage image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Graphics2D gr = image.createGraphics();
		gr.setComposite(AlphaComposite.Clear);
		gr.fillRect(0, 0, w, h);
		
		gr.setComposite(AlphaComposite.Src);
		gr.setPaint(Color.BLUE);
		gr.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
		gr.setFont(new Font("Segoe Script", Font.BOLD + Font.ITALIC, 15));
		gr.drawString(displayText, 10, 128);
		try {
			ImageIO.write(image, "PNG", baos);
		} catch (IOException e) {
			logger.info("Failed clearing out non-client response buffer due to: " + e, e);
			e.printStackTrace();
		}
		return baos.toByteArray();
	}

	public static byte[] getBytes(final ByteBuffer buffer) {
		byte[] dest = new byte[buffer.remaining()];
		buffer.get(dest);
		return dest;
	}

	public interface DateValidator {
		boolean isValid(String dateStr);
	}

	public static class DateValidatorUsingLocalDate implements DateValidator {
		private DateTimeFormatter dateFormatter;

		public DateValidatorUsingLocalDate(DateTimeFormatter dateFormatter) {
			this.dateFormatter = dateFormatter;
		}

		@Override
		public boolean isValid(String dateStr) {
			try {
				LocalDate.parse(dateStr, this.dateFormatter);
			} catch (DateTimeParseException e) {
				return false;
			}
			return true;
		}
	}

	public static ByteArrayInputStream getImageTileFromContent(final DownloadableContent content, LongParameter z,
			LongParameter x, LongParameter y) {
		final GenericData genericData = new GenericData() {
			@Override
			protected void toString(Object datum, StringBuilder buffer) {

				// Since these types are not quoted and produce a malformed JSON string, quote
				// it here.
				String d = String.valueOf(datum);
				DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;
				DateValidator validator = new DateValidatorUsingLocalDate(dateFormatter);
				if (validator.isValid(d)) {
					buffer.append("\"").append(datum).append("\"");
					return;
				}
				// For other date time format
				if (datum instanceof LocalDate || datum instanceof LocalTime || datum instanceof DateTime) {
					buffer.append("\"").append(datum).append("\"");
					return;
				}
				super.toString(datum, buffer);
			}
		};
		genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
		genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
		genericData.addLogicalTypeConversion(new TimeConversions.TimeConversion());
		genericData.addLogicalTypeConversion(new TimeConversions.TimestampConversion());
		final DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(null, null, genericData);

		ByteArrayInputStream bais = null;
		try (final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(content.getContent(),
				datumReader)) {
			while (dataFileReader.hasNext()) {
				final GenericData.Record record = dataFileReader.next();
				Long zoom = Long.parseLong(record.get("zoom_level").toString());
				Long column = Long.parseLong(record.get("tile_column").toString());
				Long row = Long.parseLong(record.get("tile_row").toString());
				if ((zoom == z.getLong()) && (column == x.getLong()) && (row == y.getLong())) {
					bais = new ByteArrayInputStream(getBytes((ByteBuffer) record.get("tile_data")));
					break;
				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (bais == null) { // set default Tiles without data
			String markedText = "";
			bais = new ByteArrayInputStream(createBlankTiles(256, 256, markedText));
		}

		return bais;

	}

}
