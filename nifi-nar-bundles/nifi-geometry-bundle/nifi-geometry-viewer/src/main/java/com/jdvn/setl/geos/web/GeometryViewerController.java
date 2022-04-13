
package com.jdvn.setl.geos.web;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.web.ViewableContent;

public class GeometryViewerController extends HttpServlet {

    /**
     * Handles generating markup for viewing an image.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        final ViewableContent content = (ViewableContent) request.getAttribute(ViewableContent.CONTENT_REQUEST_ATTRIBUTE);

        // handle images
        if ("image/png".equals(content.getContentType()) || "image/jpeg".equals(content.getContentType()) || "image/gif".equals(content.getContentType())) {
            // defer to the jsp
            request.getRequestDispatcher("/WEB-INF/jsp/image.jsp").include(request, response);
        } else {
            final PrintWriter out = response.getWriter();
            out.println("Unexpected content type: " + content.getContentType());
        }
    }
}
