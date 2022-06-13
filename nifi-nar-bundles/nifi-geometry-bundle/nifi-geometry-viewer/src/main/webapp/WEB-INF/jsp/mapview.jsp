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
	var crs = '<%= request.getAttribute("crs")%>';
	L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
	}).addTo(map);	
	var geojson = {
		  type: "FeatureCollection",
		  features: [],
		};
	function getGeometryField(json){
		for(var k in json){
			if (json[k].toUpperCase().includes("LINESTRING") || json[k].toUpperCase().includes("POINT") || json[k].toUpperCase().includes("POLYGON"))
				return k;
			else
				return null;
		}
	}	
	for (var i = 0 ; i < featureCollection.length ; i++) {
		var wkt = new Wkt.Wkt();
		var geom_field = getGeometryField(featureCollection[i]);
		if (geom_field){
			wkt.read(featureCollection[i][geom_field]);
			var properties = {};
			for (name in featureCollection[i]) {
				if (name != geom_field)
					properties[name] = featureCollection[i][name];
			}
			for (var j = 0 ; j < wkt.components.length ; j++) {
				for (var k = 0 ; k < wkt.components[j].length ; k++) {
					for (var p = 0 ; p < wkt.components[j][k].length ; p++) {
						wkt.components[j][k][p] = proj4(crs,'EPSG:4326',wkt.components[j][k][p]);
					}
				}
			}
			geojson.features.push({
			"type": "Feature",
			"geometry": wkt.toJson(),
			"properties": properties
			});
		}
	}
	console.log(geojson);
	let geoJsonLayer = L.geoJSON(geojson, {style: geomStyle}).addTo(map);
	map.fitBounds(geoJsonLayer.getBounds());
</script>