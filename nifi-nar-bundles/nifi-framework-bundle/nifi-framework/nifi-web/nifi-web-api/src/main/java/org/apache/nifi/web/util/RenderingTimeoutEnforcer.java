package org.apache.nifi.web.util;
import java.awt.Graphics;
import java.util.Timer;
import java.util.TimerTask;

import org.geotools.map.MapContent;
import org.geotools.renderer.GTRenderer;

public class RenderingTimeoutEnforcer {
    
    long timeout;
    private final GTRenderer renderer;
    private final Graphics graphics;
    private final MapContent map;
    Timer timer;
    boolean timedOut = false;

    public RenderingTimeoutEnforcer(long timeout, final GTRenderer renderer, final Graphics graphics, final MapContent map) {
        this.timeout = timeout;
        this.renderer = renderer;
        this.graphics = graphics;
        this.map = map;
    }

    /**
     * Starts checking the rendering timeout (if timeout is positive, does nothing otherwise)
     */
    public void start() {
        if(timer != null)
            throw new IllegalStateException("The timeout enforcer has already been started");
        
        if(timeout > 0) {
            timedOut = false;
            timer = new Timer();
            timer.schedule(new StopRenderingTask(), timeout);
        }
    }
    
    /**
     * Stops the timeout check
     */
    public void stop() {
        if(timer != null) {
            timer.cancel();
            timer.purge();

            timer = null;
        }
    }
    
    /**
     * Returns true if the renderer has been stopped mid-way due to the timeout occurring
     */
    public boolean isTimedOut() {
        return timedOut;
    }
    
    class StopRenderingTask extends TimerTask {

        @Override
        public void run() {
            // mark as timed out
            timedOut = true;
            renderer.stopRendering();

            graphics.dispose();
            map.dispose();
            
        }
        
    }

}