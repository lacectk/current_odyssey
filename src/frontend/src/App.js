import React, { useState } from 'react';
import { LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import { Box, Container } from '@mui/material';
import Map from './components/Map';

function App() {
  const [selectedMonth, setSelectedMonth] = useState(null);

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <Container>
        <Box sx={{ my: 4 }}>
          <DatePicker
            views={['year', 'month']}
            label="Select Month"
            value={selectedMonth}
            onChange={(newValue) => setSelectedMonth(newValue)}
            sx={{ mb: 2 }}
          />
        </Box>
        <Box sx={{ height: '600px' }}>
          <Map selectedMonth={selectedMonth} />
        </Box>
      </Container>
    </LocalizationProvider>
  );
}

export default App;
