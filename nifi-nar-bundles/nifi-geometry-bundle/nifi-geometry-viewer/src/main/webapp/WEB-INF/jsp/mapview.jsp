<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.8.0/dist/leaflet.css" type="text/css"/>
<script type="text/javascript" src="https://unpkg.com/leaflet@1.8.0/dist/leaflet.js"></script> 
<div id="map" style="height: 800px; width:800px; position: relative; padding: 0px; margin: 0 auto 0 auto;"></div>

<script> 

	var myStyle = {
		    "color": "#000",
		    "fillColor": "#ff7800",
		    "weight": 3,
		    "opacity": 0.75
		};
	var geojson = <%= request.getAttribute("content")%>;
	var map = L.map('map').setView([20.982,105.721], 12);
	L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
	    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
	}).addTo(map);	
	L.geoJSON(geojson, {
	    style: myStyle
	}).addTo(map);
	
</script>