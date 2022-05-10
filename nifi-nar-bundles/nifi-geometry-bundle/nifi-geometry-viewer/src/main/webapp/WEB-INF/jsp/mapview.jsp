<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.8.0/dist/leaflet.css" type="text/css"/>
<script type="text/javascript" src="https://unpkg.com/leaflet@1.8.0/dist/leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/wicket/1.3.6/wicket.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/wicket/1.3.6/wicket-leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/proj4js/2.8.0/proj4.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/proj4leaflet/1.0.2/proj4leaflet.min.js"></script>
<div id="map" style="height: 800px; width:800px; position: relative; padding: 0px; margin: 0 auto 0 auto;"></div>

<script> 

	var geomStyle = {
		    "color": "#000",
		    "fillColor": "#ff7800",
		    "weight": 3
		};
	var featureCollection = <%= request.getAttribute("content")%>;
	var map = L.map('map').setView([20.982,105.721], 12);

	proj4.defs([
		  [
			'EPSG:3405',
			'+proj=utm +zone=48 +ellps=WGS84 +towgs84=-192.873,-39.382,-111.202,-0.00205,-0.0005,0.00335,0.0188 +units=m +no_defs'],
		  [
			'EPSG:3857',
			'+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs'],
		  [
			'EPSG:5179',
			'+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs'],
		  [
			'EPSG:4326',
			'+title=WGS 84 (long/lat) +proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees']
		]);
		
	var vn2000Prj = 'PROJCS["VN-2000 / UTM zone 48N",GEOGCS["VN-2000",DATUM["Vietnam_2000",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[-192.873,-39.382,-111.202,-0.00205,-0.0005,0.00335,0.0188],AUTHORITY["EPSG","6756"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4756"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",105.5],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","3405"]]';
    var vn2000Prj_BN = '+proj=tmerc +lat_0=0 +lon_0=105.5 +k=0.9999 +x_0=500000 +y_0=0 +ellps=WGS84 +towgs84=-192.873,-39.382,-111.202,-0.00205,0.0005,-0.00335,0.0188000000012067 +units=m +no_defs +type=crs';
	
	var korea2000Prj = 'PROJCS["VN-2000 / UTM zone 48N",GEOGCS["VN-2000",DATUM["Vietnam_2000",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[-192.873,-39.382,-111.202,-0.00205,-0.0005,0.00335,0.0188],AUTHORITY["EPSG","6756"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4756"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",105.5],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","3405"]]';	
	var korea2000 = '+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs';
		
	
	L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
	}).addTo(map);	
	var geojson = {
		  type: "FeatureCollection",
		  features: [],
		};
	for (var i = 0 ; i < featureCollection.length ; i++) {
		var wkt = new Wkt.Wkt();
		wkt.read(featureCollection[i].the_geom);
		//console.log(wkt.components);
		
		var properties = {};
		for (name in featureCollection[i]) {
			if (name.includes("geom") != true)
				properties[name] = featureCollection[i][name];
		}
		for (var j = 0 ; j < wkt.components.length ; j++) {
			for (var k = 0 ; k < wkt.components[j].length ; k++) {
				for (var p = 0 ; p < wkt.components[j][k].length ; p++) {
					wkt.components[j][k][p] = proj4(vn2000Prj_BN,'EPSG:4326',wkt.components[j][k][p]);
				}
			}
		}
		geojson.features.push({
		"type": "Feature",
		"geometry": wkt.toJson(),
		"properties": properties
		});
		
	}
	let geoJsonLayer = L.geoJSON(geojson, {style: geomStyle}).addTo(map);
	map.fitBounds(geoJsonLayer.getBounds());
	
</script>