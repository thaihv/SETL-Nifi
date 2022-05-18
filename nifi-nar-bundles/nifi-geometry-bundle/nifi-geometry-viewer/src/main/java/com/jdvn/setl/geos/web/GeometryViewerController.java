
package com.jdvn.setl.geos.web;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordReader;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.web.ViewableContent;
import org.apache.nifi.web.ViewableContent.DisplayMode;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GeometryViewerController extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static final Set<String> supportedMimeTypes = new HashSet<>();
	
    static {
        supportedMimeTypes.add("application/avro+geowkt");
    }

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		final ViewableContent content = (ViewableContent) request
				.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);

		// handle json/xml specifically, treat others as plain text
		String contentType = content.getContentType();
		if (supportedMimeTypes.contains(contentType)) {
			final String formatted;

			// leave the content alone if specified
			if (DisplayMode.Original.equals(content.getDisplayMode())) {
				formatted = content.getContent();
				request.setAttribute("mode", contentType);
				request.setAttribute("content", formatted);
				request.getRequestDispatcher("/WEB-INF/jsp/geometry.jsp").include(request, response);
			} else {
				if ("application/avro+geowkt".equals(contentType)) {		            
					final StringBuilder sb = new StringBuilder();
					sb.append("[");
					// Use Avro conversions to display logical type values in human readable way.
					final GenericData genericData = new GenericData() {
						@Override
						protected void toString(Object datum, StringBuilder buffer) {
							// Since these types are not quoted and produce a malformed JSON string, quote
							// it here.
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
					final DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(null, null,
							genericData);
					try (final DataFileStream<GenericData.Record> dataFileReader = new DataFileStream<>(
							content.getContentStream(), datumReader)) {
						while (dataFileReader.hasNext()) {
							final GenericData.Record record = dataFileReader.next();
							final String formattedRecord = genericData.toString(record);
							sb.append(formattedRecord);
							sb.append(",");
							// Do not format more than 16 MB of content.
							if (sb.length() > 1024 * 1024 * 16) {
								break;
							}
						}
					}

					if (sb.length() > 1) {
						sb.deleteCharAt(sb.length() - 1);
					}
					sb.append("]");
					final String json = sb.toString();

					final ObjectMapper mapper = new ObjectMapper();
					final Object objectJson = mapper.readValue(json, Object.class);
					formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJson);

					
					// defer to the jsp
					contentType = "application/json";
					request.setAttribute("mode", contentType);
					request.setAttribute("content", formatted);
					request.setAttribute("crs", request.getAttribute(ViewableContent.GEO_CONTENT_CRS));
					request.getRequestDispatcher("/WEB-INF/jsp/mapview.jsp").include(request, response);

				} else {
					// leave plain text alone when formatting
					formatted = content.getContent();
					request.setAttribute("mode", contentType);
					request.setAttribute("content", formatted);
					request.getRequestDispatcher("/WEB-INF/jsp/geometry.jsp").include(request, response);
				}
			}
			
		} else {
			final PrintWriter out = response.getWriter();
			out.println("Unexpected content type: " + contentType);
		}
	}
    // create the image
    private BufferedImage createClockImage() {
        GregorianCalendar cal = new GregorianCalendar();

        BufferedImage img = new BufferedImage(400, 400, BufferedImage.TYPE_INT_RGB);
        Graphics2D g = img.createGraphics();

        // white background
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, 400, 400);

        // draw black circle around clock
        g.setColor(Color.BLACK);
        g.setStroke(new BasicStroke(5));
        g.drawOval(20, 20, 360, 360);

        // draw hour hand
        double hourRad = cal.get(Calendar.HOUR) * 2 * Math.PI / 12 - 0.5 * Math.PI;
        g.drawLine(200, 200, 200 + (int) (100 * Math.cos(hourRad)), 
                   200 + (int) (100 * Math.sin(hourRad)));

        // draw minute hand
        double minuteRad = cal.get(Calendar.MINUTE) * 2 * Math.PI / 60 - 0.5 * Math.PI;
        g.drawLine(200, 200, 200 + (int) (170 * Math.cos(minuteRad)), 
                   200 + (int) (170 * Math.sin(minuteRad)));
        return img;
    }    
    public BufferedImage imageFromRecordSet(InputStream in) {
    	int imageType = BufferedImage.TYPE_INT_RGB;
    	final BufferedImage bufferedImg = new BufferedImage(800, 800, imageType);
		try {
            final AvroRecordReader reader = new AvroReaderWithEmbeddedSchema(in);
            final RecordSchema recordSchema = reader.getSchema();
            Record record = reader.nextRecord();
            RecordFieldType type = recordSchema.getDataType("the_geom").get().getFieldType();
//            System.out.println(recordSchema);
//            System.out.println(type);
//            System.out.println(record);
            final Graphics2D graphics = bufferedImg.createGraphics();
            graphics.setPaint(Color.BLUE);
            graphics.setStroke(new BasicStroke(1.5f));
            try {
                graphics.drawString("Hello, Under construction!", 50, 50);
            } finally {
                graphics.dispose();
            }
            
		} catch (IOException | MalformedRecordException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
   	
		return bufferedImg;
    	
    }

}
