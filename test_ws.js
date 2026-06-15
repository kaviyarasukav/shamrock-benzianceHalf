const WebSocket = require('ws');

const urls = [
  'wss://nbstream.binance.com/eoptions/stream',
  'wss://nbstream.binance.com/eoptions/stream?streams=!trade@arr',
  'wss://eoptions.binance.com/stream?streams=!trade@arr',
  'wss://eoptions.binance.com/ws',
  'wss://nbstream.binance.com/eoptions/ws'
];

urls.forEach(url => {
  const ws = new WebSocket(url);
  ws.on('open', () => {
    console.log(`Success: ${url}`);
    ws.close();
  });
  ws.on('error', (err) => {
    console.log(`Error on ${url}: ${err.message}`);
  });
});
