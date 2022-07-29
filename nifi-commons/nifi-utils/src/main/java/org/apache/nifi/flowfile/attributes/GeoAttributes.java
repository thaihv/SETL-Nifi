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
package org.apache.nifi.flowfile.attributes;

public enum GeoAttributes implements FlowFileAttributeKey {

    /**
     * The information in WKT format of coordinate reference system of the FlowFile if it is geo-spatial data 
     */
    CRS("crs"),
    /**
     * The data type of geo-spatial: Tiles or Features 
     */
    GEO_TYPE("geo.type"),
    /**
     * The format type of image Tiles 
     */
    GEO_RASTER_TYPE("geo.raster.format"),
    /**
     * The boundary of geo spatial 
     */
    GEO_ENVELOPE("geo.envelope"),
    /**
     * The center of geo boundary 
     */
    GEO_CENTER("geo.center"),    
    /**
     * The min level of zoom 
     */
    GEO_ZOOM_MIN("geo.zoomin"),
    /**
     * The max level of zoom 
     */
    GEO_ZOOM_MAX("geo.zoomax"),
    /**
     * The number of geo records 
     */
    GEO_RECORD_NUM("geo.records.num"),    
    /**
     * The tile matrix of geo spatial that will compressed to reduce size for storage 
     */
    GEO_TILE_MATRIX("geo.raster.tilematrix"),   
    /**
     * The name of Tile or Feature 
     */
    GEO_NAME("geo.name");    
	
    private final String key;

    private GeoAttributes(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

}
