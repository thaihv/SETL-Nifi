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
package org.apache.nifi.web;

import java.io.InputStream;

/**
 * Represents content that can be downloaded.
 */
public final class DownloadableContent {

    private final String filename;
    private final String type;
    private String crs = null;
    private String geoType = null;
    private String geoImageType = null;
    private final InputStream content;

    public DownloadableContent(String filename, String type, InputStream content) {
        this.filename = filename;
        this.type = type;
        this.content = content;
    }

    /**
     * The filename of the content.
     *
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * The content type of the content.
     *
     * @return the content type
     */
    public String getType() {
		if (geoType.contentEquals("Features")) {
			return "application/avro+geowkt";
		}
		else if (geoType.contentEquals("Tiles")){
			return "application/avro+geotiles";
		}    	
    	return type;
    }
    
    /**
     * The coordinate system information of the geo content.
     *
     * @return the content crs
     */
    public String getCrs() {
		return crs;
	}
    /**
     * Set coordinate system information for the geo content.
     *
     * @param the crs of geo content
     */
	public void setCrs(String crs) {
		this.crs = crs;
	}

	public String getGeoType() {
		return geoType;
	}

	public void setGeoType(String geoType) {
		this.geoType = geoType;
	}

	public String getGeoImageType() {
		return geoImageType;
	}

	public void setGeoImageType(String geoImageType) {
		this.geoImageType = geoImageType;
	}

	public boolean isGeoContent() {
		if (geoType.contentEquals("Features") || geoType.contentEquals("Tiles"))
			return true;
		return false;
		
	}
	/**
     * The content stream.
     *
     * @return the input stream of the content
     */
    public InputStream getContent() {
        return content;
    }
}
