<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.8.0/dist/leaflet.css" type="text/css"/>
<script type="text/javascript" src="https://unpkg.com/leaflet@1.8.0/dist/leaflet.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/wicket/1.3.6/wicket.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/wicket/1.3.6/wicket-leaflet.js"></script>
<div id="map" style="height: 800px; width:800px; position: relative; padding: 0px; margin: 0 auto 0 auto;"></div>

<script> 

	var geomStyle = {
		    "color": "#000",
		    "fillColor": "#ff7800",
		    "weight": 3
		};
	var featureCollection = <%= request.getAttribute("content")%>;
	var map = L.map('map').setView([20.982,105.721], 12);
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
		var properties = {};
		for (name in featureCollection[i]) {
			if (name.includes("geom") != true) 
				properties[name] = featureCollection[i][name];
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