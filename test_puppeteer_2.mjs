import puppeteer from 'puppeteer';

(async () => {
  const browser = await puppeteer.launch({ args: ['--no-sandbox'] });
  const page = await browser.newPage();
  
  page.on('console', msg => {
      console.log('PAGE LOG:', msg.text());
  });
  
  await page.goto('http://127.0.0.1:3000/');
  await new Promise(r => setTimeout(r, 8000));
  await browser.close();
})();
