package org.apache.nifi.web.util;

import java.awt.Graphics;

import org.geotools.map.MapContent;
import org.geotools.renderer.GTRenderer;
import org.geotools.renderer.RenderListener;
import org.opengis.feature.simple.SimpleFeature;

public class MaxErrorEnforcer {
	private final GTRenderer renderer;
	private final Graphics graphics;
	private final MapContent map;
    int maxErrors;

    int errors;
    
    Exception lastException;

    /**
     * Builds a new max errors enforcer. If maxErrors is not positive the enforcer will do nothing
     * 
     * @param renderer
     * @param maxErrors
     */
    public MaxErrorEnforcer(final GTRenderer renderer, final Graphics graphics, final MapContent map, int maxErrors) {
        this.renderer = renderer;
        this.graphics = graphics;
        this.map = map;
        this.maxErrors = maxErrors;
        this.errors = 0;

        if (maxErrors > 0) {
            renderer.addRenderListener(new RenderListener() {

                public void featureRenderer(SimpleFeature feature) {
                }

                public void errorOccurred(Exception e) {
                    errors++;
                    lastException = e;
                    if (errors > MaxErrorEnforcer.this.maxErrors) {
                        MaxErrorEnforcer.this.renderer.stopRendering();
                        MaxErrorEnforcer.this.graphics.dispose();
                        MaxErrorEnforcer.this.map.dispose();
                    }
                }
            });
        }
    }

    /**
     * True if the max error threshold was exceeded
     * @return
     */
    public boolean exceedsMaxErrors() {
        return maxErrors > 0 && errors > maxErrors;
    }
    
    /**
     * Returns the last exception occurred (or null if none happened)
     * @return
     */
    public Exception getLastException() {
        return lastException;
    }
}
