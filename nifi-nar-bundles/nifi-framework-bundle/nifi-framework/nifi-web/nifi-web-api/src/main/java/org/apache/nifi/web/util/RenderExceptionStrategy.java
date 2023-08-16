package org.apache.nifi.web.util;
import java.awt.Graphics;
import java.awt.geom.NoninvertibleTransformException;

import org.apache.nifi.logging.NiFiLog;
import org.geotools.map.MapContent;
import org.geotools.renderer.GTRenderer;
import org.geotools.renderer.RenderListener;
import org.opengis.feature.IllegalAttributeException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RenderExceptionStrategy implements RenderListener {

	private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(RenderExceptionStrategy.class));
	
    private final GTRenderer renderer;
    private final Graphics graphics;
    private final MapContent map;
    
    private Exception renderException;

    public RenderExceptionStrategy(final GTRenderer renderer, final Graphics graphics, final MapContent map) {
        this.renderer = renderer;
        this.graphics = graphics;
        this.map = map;
        this.renderException = null;
    }

    public boolean exceptionOccurred() {
        return renderException != null;
    }

    public Exception getException() {
        return renderException;
    }

    public void errorOccurred(final Exception renderException) {

        Throwable cause = renderException;
        
		while (cause != null) {
			if (cause instanceof TransformException || cause instanceof IllegalAttributeException
					|| cause instanceof FactoryException || cause instanceof NoninvertibleTransformException) {

				logger.info("RenderExceptionStrategy --> Ignoring renderer error", renderException);
				return;
			}
			cause = cause.getCause();
		}

		logger.info("RenderExceptionStrategy --> Got an unexpected render exception.", renderException);
        this.renderException = renderException;
        renderer.stopRendering();
        graphics.dispose();
        map.dispose();
    }

    public void featureRenderer(SimpleFeature feature) {

    }
}