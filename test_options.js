const WebSocket = require('ws');

const urls = [
  'wss://nbstream.binance.com/eoptions/stream?streams=BTC-240426-60000-C@trade',
  'wss://nbstream.binance.com/eoptions/ws',
  'wss://nbstream.binance.com/eoptions/stream',
  'wss://eoptions.binance.com/stream'
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
