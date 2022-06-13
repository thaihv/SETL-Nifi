
package com.jdvn.setl.geos.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
					request.setAttribute("crs", request.getAttribute(ViewableContent.GEO_CONTENT_CRS).toString().replaceAll("[\\r\\n\\t ]", ""));
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

	
	public interface DateValidator {
		   boolean isValid(String dateStr);
		}
	public class DateValidatorUsingLocalDate implements DateValidator {
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

}
