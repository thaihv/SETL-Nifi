
package com.jdvn.setl.geos.processors.CRStransform;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.flowfile.attributes.GeoAttributes;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.geotools.metadata.iso.citation.Citations;
import org.geotools.referencing.CRS;
import org.geotools.referencing.wkt.Formattable;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import com.jdvn.setl.geos.processors.geotransform.CRStransform;
import com.jdvn.setl.geos.processors.util.GeoUtils;


public class CRStransformTest {

    @Test
    public void testProjectedKoreanCoordinatesToDecimalDegree() throws FactoryException, TransformException {
        //Korea EPSG:5179 -> EPSG:4326 CONVERSION

        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:5179");
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");

        double coordinateX = 1307285;
        double coordinateY = 2229260;

        // EPSG:5179 Y , X
        Coordinate in = new Coordinate(coordinateY, coordinateX); 
        Coordinate result = GeoUtils.transformCoordinateBasedOnCrs(sourceCRS,targetCRS,in);
        
        double expectedLongitude = 131.0999928;
        double expectedLatitude = 40.0099721;

        assertEquals(expectedLongitude, result.getY(), 0.00001);
        assertEquals(expectedLatitude, result.getX(), 0.00001);
    } 
    @Test
	public void listupSupportedCodes() throws FactoryException, TransformException {

		Set<String> capabilitiesCrsIdentifiers;
		capabilitiesCrsIdentifiers = new LinkedHashSet<String>();

		for (String code : CRS.getSupportedCodes("EPSG")) {
			if ("WGS84(DD)".equals(code))
				continue;
			capabilitiesCrsIdentifiers.add("EPSG:" + code);
		}
		for (String code : CRS.getSupportedCodes("ESRI")) {
			if ("WGS84(DD)".equals(code))
				continue;
			capabilitiesCrsIdentifiers.add("ESRI:" + code);
		}

		System.out.println(capabilitiesCrsIdentifiers);

	}

    @Test
    public void testUserDefinedCRSToEPSG4326() throws FactoryException, TransformException {
        //VN2000 EPSG:3405 -> EPSG:4326 CONVERSION
        String wkt = "PROJCS[\"VN_2000_UTM_Zone_48N\", \r\n" + 
        		"  GEOGCS[\"GCS_VN-2000\", \r\n" + 
        		"    DATUM[\"D_Vietnam_2000\", \r\n" + 
        		"      SPHEROID[\"WGS_1984\", 6378137.0, 298.257223563], \r\n" + 
        		"      TOWGS84[-192.873, -39.382, -111.202, -0.00205, 0.0005, -0.00335, 0.0188], \r\n" + 
        		"      AUTHORITY[\"EPSG\",\"6756\"]], \r\n" + 
        		"    PRIMEM[\"Greenwich\", 0.0], \r\n" + 
        		"    UNIT[\"degree\", 0.017453292519943295], \r\n" + 
        		"    AXIS[\"Longitude\", EAST], \r\n" + 
        		"    AXIS[\"Latitude\", NORTH], \r\n" + 
        		"    AUTHORITY[\"EPSG\",\"4756\"]], \r\n" + 
        		"  PROJECTION[\"Transverse_Mercator\"], \r\n" + 
        		"  PARAMETER[\"central_meridian\", 107.75], \r\n" + 
        		"  PARAMETER[\"latitude_of_origin\", 0.0], \r\n" + 
        		"  PARAMETER[\"scale_factor\", 0.9999], \r\n" + 
        		"  PARAMETER[\"false_easting\", 500000.0], \r\n" + 
        		"  PARAMETER[\"false_northing\", 0.0], \r\n" + 
        		"  UNIT[\"m\", 1.0], \r\n" + 
        		"  AXIS[\"x\", EAST], \r\n" + 
        		"  AXIS[\"y\", NORTH], \r\n" + 
        		"  AUTHORITY[\"VN\",\"10775\"]]";
        CoordinateReferenceSystem sourceCRS = CRS.parseWKT(wkt);
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:4326");

        double coordinateX = 547486.6400756836;
        double coordinateY = 2348240.210083008;
        // EPSG:3405 X , Y
        Coordinate in = new Coordinate(coordinateX, coordinateY); 
        Coordinate result = GeoUtils.transformCoordinateBasedOnCrs(sourceCRS,targetCRS,in);

        double expectedLongitude = 108.20935292107634;
        double expectedLatitude =  21.22776257853827;
   
        assertEquals(expectedLongitude, result.getY(), 0.00001);
        assertEquals(expectedLatitude, result.getX(), 0.00001);
        
        Formattable f = (Formattable) CRS.decode("EPSG:32735", true);
        wkt = f.toWKT(Citations.ESRI, 0); // use 0 indent for single line// 2

        System.out.println("wkt for EPSG:32735 (ESRI)");
        System.out.println(wkt);
        
    }       
    @Test
    public void testTransformResults() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new CRStransform());
        runner.setValidateExpressionUsage(false);

        runner.setProperty(CRStransform.USER_DEFINED, "false");
        runner.setProperty(CRStransform.CRS_TARGET, "EPSG:4326");
        
        
        final List<Field> buildingFields = new ArrayList<>();
        buildingFields.add(new Field("the_geom", Schema.create(Type.STRING), null, (Object) null));
        buildingFields.add(new Field("ROAD_NM", Schema.create(Type.STRING), null, (Object) null));
        buildingFields.add(new Field("BUL_MAN_NO", Schema.create(Type.LONG), null, (Object) null));
        buildingFields.add(new Field("BDTYP_CD", Schema.create(Type.STRING), null, (Object) null));
        final Schema schema = Schema.createRecord("Buildings", null, null, false);
        schema.setFields(buildingFields);
        
        final byte[] source;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            final DataFileWriter<GenericRecord> writer = dataFileWriter.create(schema, baos)) {


            final GenericRecord b1Record = new GenericData.Record(schema);
            b1Record.put("the_geom", "MULTIPOLYGON (((957993.9864196742 1945576.9570123316, 957983.4609443424 1945573.108225389, 957980.7670381214 1945580.2958235308, 957991.3514169772 1945584.31923318, 957993.9864196742 1945576.9570123316)))");
            b1Record.put("ROAD_NM", "Hakdong-ro 4-gil");
            b1Record.put("BUL_MAN_NO", 21345L);
            b1Record.put("BDTYP_CD", "01001");
            writer.append(b1Record);
            writer.flush();
            
            final GenericRecord b2Record = new GenericData.Record(schema);
            b2Record.put("the_geom", "MULTIPOLYGON (((958025.2624564759 1945601.266386887, 958013.7439423987 1945596.9630322922, 958010.4903963841 1945605.5000932196, 958022.2040601482 1945609.8453978966, 958025.2624564759 1945601.266386887)))");
            b2Record.put("ROAD_NM", "Hakdong-ro 5-gil");
            b2Record.put("BUL_MAN_NO", 21347L);
            b2Record.put("BDTYP_CD", "01011");
            writer.append(b2Record);
            writer.flush();
        }        
        source = baos.toByteArray();
        String wkt = "PROJCS[\"Korea 2000 / Unified CS\", \r\n" + 
        		"  GEOGCS[\"Korea 2000\", \r\n" + 
        		"    DATUM[\"Geocentric datum of Korea\", \r\n" + 
        		"      SPHEROID[\"GRS 1980\", 6378137.0, 298.257222101, AUTHORITY[\"EPSG\",\"7019\"]], \r\n" + 
        		"      TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], \r\n" + 
        		"      AUTHORITY[\"EPSG\",\"6737\"]], \r\n" + 
        		"    PRIMEM[\"Greenwich\", 0.0], \r\n" + 
        		"    UNIT[\"degree\", 0.017453292519943295], \r\n" + 
        		"    AXIS[\"Longitude\", EAST], \r\n" + 
        		"    AXIS[\"Latitude\", NORTH], \r\n" + 
        		"    AUTHORITY[\"EPSG\",\"4737\"]], \r\n" + 
        		"  PROJECTION[\"Transverse_Mercator\"], \r\n" + 
        		"  PARAMETER[\"central_meridian\", 127.5], \r\n" + 
        		"  PARAMETER[\"latitude_of_origin\", 38.0], \r\n" + 
        		"  PARAMETER[\"scale_factor\", 0.9996], \r\n" + 
        		"  PARAMETER[\"false_easting\", 1000000.0], \r\n" + 
        		"  PARAMETER[\"false_northing\", 2000000.0], \r\n" + 
        		"  UNIT[\"m\", 1.0], \r\n" + 
        		"  AXIS[\"x\", EAST], \r\n" + 
        		"  AXIS[\"y\", NORTH], \r\n" + 
        		"  AUTHORITY[\"EPSG\",\"5179\"]]";
        Map<String, String> attributes = new HashMap<>();
        attributes.put(GeoAttributes.CRS.key(), wkt);
        attributes.put(GeoUtils.GEO_URL, "from flowfile");
        
        runner.enqueue(source, attributes);
        runner.run();

    }
}
