import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import axios from 'axios';
import 'leaflet/dist/leaflet.css';

function Map({ selectedMonth }) {
  const [consistencyData, setConsistencyData] = useState(null);

  useEffect(() => {
    if (!selectedMonth) return;

    const fetchConsistencyData = async () => {
      try {
        const formattedMonth = selectedMonth.format('YYYY-MM');
        const response = await axios.get(`http://localhost:8000/consistency`, {
          params: {
            start_month: formattedMonth,
            end_month: formattedMonth
          }
        });
        setConsistencyData(response.data);
      } catch (error) {
        console.error('Error fetching consistency data:', error);
      }
    };

    fetchConsistencyData();
  }, [selectedMonth]);

  const getColorForConsistency = (score) => {
    // Convert score (0-1) to color
    if (score >= 0.8) return '#ff0000';      // red
    if (score >= 0.6) return '#ffff00';      // yellow
    if (score >= 0.4) return '#00ff00';      // lime
    if (score >= 0.2) return '#00ffff';      // cyan
    return '#4169e1';                        // royalblue
  };

  return (
    <MapContainer 
      center={[37.7749, -122.4194]}  // Default to San Francisco
      zoom={4} 
      style={{ height: '600px', width: '100%' }}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; OpenStreetMap contributors'
      />
      {consistencyData && consistencyData.map((point, index) => (
        <CircleMarker
          key={index}
          center={[point.latitude, point.longitude]}
          radius={10}
          fillColor={getColorForConsistency(point.consistency_score)}
          color="#000"
          weight={1}
          opacity={1}
          fillOpacity={0.7}
        >
          <Popup>
            <div>
              <strong>Consistency Score:</strong> {point.consistency_score.toFixed(2)}
              <br />
              <strong>Location:</strong> {point.latitude.toFixed(2)}°, {point.longitude.toFixed(2)}°
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}

export default Map;