import http from 'http';

http.get('http://127.0.0.1:3000/api/settings/autopilot', (res) => {
  let data = '';
  res.on('data', chunk => data += chunk);
  res.on('end', () => console.log('HTTP', res.statusCode, data.substring(0, 500)));
}).on('error', err => console.error('Error', err));
