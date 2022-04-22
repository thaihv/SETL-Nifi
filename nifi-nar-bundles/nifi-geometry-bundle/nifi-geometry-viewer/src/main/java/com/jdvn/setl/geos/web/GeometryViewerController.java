
package com.jdvn.setl.geos.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.web.ViewableContent;
import org.apache.nifi.web.ViewableContent.DisplayMode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GeometryViewerController extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private static final Set<String> supportedMimeTypes = new HashSet<>();

    static {
        supportedMimeTypes.add("application/avro+geowkt");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ViewableContent content = (ViewableContent) request.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);

        // handle json/xml specifically, treat others as plain text
        String contentType = content.getContentType();
        if (supportedMimeTypes.contains(contentType)) {
            final String formatted;

            // leave the content alone if specified
            if (DisplayMode.Original.equals(content.getDisplayMode())) {
                formatted = content.getContent();
            } else {
                if ("application/avro+geowkt".equals(contentType)) {
                    // format json
                    final ObjectMapper mapper = new ObjectMapper();
//                    final Object objectJson = mapper.readValue(content.getContentStream(), Object.class);
//                    formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectJson);
                    
                    String json = "{ \"reason\" : \"Under Construction\", \"type\" : \"Geometry\" }";
                    JsonNode jsonNode = mapper.readTree(json);
                    formatted = jsonNode.get("reason").asText();
                    
                    contentType = "application/json";
                } else {
                    // leave plain text alone when formatting
                    formatted = content.getContent();
                }
            }

            // defer to the jsp
            request.setAttribute("mode", contentType);
            request.setAttribute("content", formatted);
            request.getRequestDispatcher("/WEB-INF/jsp/geometry.jsp").include(request, response);
        } else {
            final PrintWriter out = response.getWriter();
            out.println("Unexpected content type: " + contentType);
        }
    }
}
