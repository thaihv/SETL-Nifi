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
import java.awt.Rectangle;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.avro.Conversions;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.api.request.LongParameter;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.referencing.CRS;
import org.geotools.renderer.GTRenderer;
import org.geotools.renderer.label.LabelCacheImpl;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.AnchorPoint;
import org.geotools.styling.Displacement;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.Fill;
import org.geotools.styling.Graphic;
import org.geotools.styling.LineSymbolizer;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.PolygonSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.Stroke;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactory;
import org.joda.time.DateTime;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.style.GraphicalSymbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class GeoUtils {

	private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(GeoUtils.class));

	public static BoundingBox tile2boundingBox(final int x, final int y, final int zoom) {
		BoundingBox bb = new BoundingBox();
		bb.north = tile2lat(y, zoom);
		bb.south = tile2lat(y + 1, zoom);
		bb.west = tile2lon(x, zoom);
		bb.east = tile2lon(x + 1, zoom);
		return bb;
	}

	private static double tile2lon(int x, int z) {
		return x / Math.pow(2.0, z) * 360.0 - 180;
	}

	private static double tile2lat(int y, int z) {
		double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
		return Math.toDegrees(Math.atan(Math.sinh(n)));
	}
	
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
    private static byte[] imageFromFeatures(SimpleFeatureCollection fc, ReferencedEnvelope bounds, Style style, int w, int h) {

		BufferedImage image = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (bounds != null) {
    		Graphics2D gr = image.createGraphics();
    		gr.setComposite(AlphaComposite.Clear);
    		gr.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
    		if (fc != null) {
        		MapContent map = new MapContent();
                Layer layer = new FeatureLayer(fc, style);
                map.addLayer(layer);
                
                Rectangle outputArea = new Rectangle(w, h);    	
                GTRenderer renderer = new StreamingRenderer();
                LabelCacheImpl labelCache = new LabelCacheImpl();
                Map<Object, Object> hints = renderer.getRendererHints();
                
                if (hints == null) {
                  hints = new HashMap<>();
                }
                hints.put(StreamingRenderer.LABEL_CACHE_KEY, labelCache);
                renderer.setRendererHints(hints);
                renderer.setMapContent(map);
                renderer.paint(gr, outputArea, bounds); 
                try {
        			ImageIO.write(image, "PNG", baos);
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
                map.dispose();       			
    		}
        }
        return baos.toByteArray();
    }
    private static Style createStyle() {
    	
    	StyleFactory styleFactory = CommonFactoryFinder.getStyleFactory();
    	FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2();
    	
        PolygonSymbolizer polySymbolizer = styleFactory.createPolygonSymbolizer();
        Fill fill = styleFactory.createFill(
                filterFactory.literal("#FFAA00"),
                filterFactory.literal(0.25)
        );
        final Stroke stroke = styleFactory.createStroke(filterFactory.literal(Color.BLACK), filterFactory.literal(2));
        polySymbolizer.setFill(fill);
        polySymbolizer.setStroke(stroke);
        

        Expression opacity = null; // use default
        Expression size = filterFactory.literal(5);
        Expression rotation = null; // use default
        AnchorPoint anchor = null; // use default
        Displacement displacement = null; // use default
        List<GraphicalSymbol> symbols = new ArrayList<>();
        symbols.add(styleFactory.mark(filterFactory.literal("circle"), fill, stroke)); 
        // define a point symbolizer of a small circle
        Graphic circle = styleFactory.graphic(symbols, opacity, size, rotation, anchor, displacement);
        PointSymbolizer pointSymbolizer = styleFactory.pointSymbolizer("point", filterFactory.property("geometry"), null, null, circle);
        
        LineSymbolizer lineSymbolizer = styleFactory.createLineSymbolizer();
        lineSymbolizer.setStroke(styleFactory.createStroke(filterFactory.literal(Color.MAGENTA), filterFactory.literal(1)));
        
        Rule rule = styleFactory.createRule();
        rule.symbolizers().add(polySymbolizer);
        rule.symbolizers().add(pointSymbolizer);
        rule.symbolizers().add(lineSymbolizer);
        FeatureTypeStyle fts = styleFactory.createFeatureTypeStyle();
        fts.rules().add(rule);

        Style style = styleFactory.createStyle();
        style.featureTypeStyles().add(fts);
        return style;
        
        
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
	public static String getGeometryFieldName(GenericData.Record record) {
		String geoKey = null;
		for (int i = 0; i < record.getSchema().getFields().size(); i++) {
			Field f = record.getSchema().getFields().get(i);
			String value = record.get(f.name()) == null ? null: record.get(f.name()).toString();
			
			if ((value != null) && (value.contains("MULTILINESTRING") || value.contains("LINESTRING") || value.contains("MULTIPOLYGON")
					|| value.contains("POLYGON") || value.contains("POINT") || value.contains("MULTIPOINT")
					|| value.contains("GEOMETRYCOLLECTION"))) {

				geoKey = f.name();
				break;
			}
		}
		return geoKey;

	}
	
	@SuppressWarnings("rawtypes")
	public static Class getTypeGeometry(GenericData.Record record) {
		Class geometryClass = Point.class; // default
		String geokey = getGeometryFieldName(record); 
		String value = record.get(geokey) == null ? null : record.get(geokey).toString();
		if (value != null) {
			value = value.substring(0, value.indexOf("(")).trim();
			switch (value) {
				case "MULTILINESTRING":
					geometryClass = MultiLineString.class;
					break;
				case "LINESTRING":
					geometryClass = LineString.class;
					break;
				case "MULTIPOLYGON":
					geometryClass = MultiPolygon.class;
					break;
				case "POLYGON":
					geometryClass = Polygon.class;
					break;
				case "MULTIPOINT":
					geometryClass = MultiPoint.class;
					break;
				case "POINT":
					geometryClass = Point.class;
					break;
				case "GEOMETRYCOLLECTION":
					geometryClass = GeometryCollection.class;
					break;
				default:
					geometryClass = Point.class;
			}			
		}		
		return geometryClass;
	}	
	public static ByteArrayInputStream getImageTileFromContent(final DownloadableContent content, LongParameter z,
			LongParameter x, LongParameter y) {
		String geoType = content.getGeoType();
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
			if (geoType.equals("Tiles")) {
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
			}
			else if (geoType.equals("Features")) {
				if (content.getCrs() != null) {
			    	List<SimpleFeature> collection = new LinkedList<SimpleFeature>();
			    	CoordinateReferenceSystem crs_source = CRS.parseWKT(content.getCrs());			    	
			    	Style style = createStyle();
			    	boolean bCreatedFeatureType = false;
			    	WKTReader2 wkt = new WKTReader2();
			    	SimpleFeatureType TYPE = null;
			    	String geokey = null;
					while (dataFileReader.hasNext()) {
						final GenericData.Record record = dataFileReader.next();
						if (bCreatedFeatureType == false) {
							geokey = getGeometryFieldName(record);
							SimpleFeatureTypeBuilder tbuilder =  new SimpleFeatureTypeBuilder();
							tbuilder.setName("Features");
					    	tbuilder.setCRS(crs_source);
					    	tbuilder.add("geometry", getTypeGeometry(record));
					    	
					    	TYPE = tbuilder.buildFeatureType();
					    	bCreatedFeatureType = true;
						}
						String wktGeo = record.get(geokey) == null ? null : record.get(geokey) .toString();
						if (wktGeo != null)
							collection.add(SimpleFeatureBuilder.build(TYPE, new Object[] { wkt.read(wktGeo)}, null));
						
					}
					
					int x0 = x.getLong().intValue();
					int y0 = y.getLong().intValue();
					int z0 = z.getLong().intValue();
					
					BoundingBox bb = tile2boundingBox(x0,y0,z0);
//					System.out.println("X/Y/Z:" + String.valueOf(x0) + "/" + String.valueOf(y0) + "/" + String.valueOf(z0));
//					System.out.println("Origin: (North;West) -->" + "(" + String.valueOf(bb.north) + ";" + String.valueOf(bb.west) + ")");
			        
					MathTransform transform = CRS.findMathTransform(CRS.decode("EPSG:4326"),crs_source);
			        GeometryFactory gf = new GeometryFactory();
			        Point nw1 = gf.createPoint(new Coordinate(bb.north, bb.west));
			        Point se1 = gf.createPoint(new Coordinate(bb.south, bb.east));
			        Point nw = (Point) JTS.transform(nw1, transform);
			        Point se = (Point) JTS.transform(se1, transform);
					
					double x1 = nw.getX();
					double x2 = se.getX();
					double y1 = nw.getY();
					double y2 = se.getY();
//					System.out.println("x1,x2,y1,y2: " + String.valueOf(x1) + "," + String.valueOf(x2) + "," + String.valueOf(y1) + "," + String.valueOf(y2));
							
					ReferencedEnvelope envelope = new ReferencedEnvelope(x1, x2, y1, y2, crs_source);
			        bais = new ByteArrayInputStream(imageFromFeatures(new ListFeatureCollection(TYPE, collection), envelope, style, 256, 256));						
				}
			}

		} catch (IOException | ParseException | FactoryException | MismatchedDimensionException | TransformException e1) {
			e1.printStackTrace();
		}
		if (bais == null) { // set default Tiles without data
			String markedText = "";
			bais = new ByteArrayInputStream(createBlankTiles(256, 256, markedText));
		}
		return bais;
	}
	static class BoundingBox {
		double north;
		double south;
		double east;
		double west;
	}
}
