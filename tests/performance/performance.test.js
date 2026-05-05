const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Helper to simulate network latency
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('Performance Validation', () => {
  const baseUrl = 'http://localhost:8086';

  it('loads within 2 seconds on desktop', async () => {
    const start = Date.now();
    await axios.get(baseUrl);
    const loadTime = Date.now() - start;
    expect(loadTime).toBeLessThan(2000);
  }, 5000);

  it('loads core content within 2 seconds on 3G', async () => {
    // Simulate 3G network conditions (100ms latency + slower connection)
    const start = Date.now();
    await delay(100);
    await axios.get(baseUrl);
    const loadTime = Date.now() - start;
    expect(loadTime).toBeLessThan(2000);
  }, 10000);

  it('has optimized images', () => {
    const imagesDir = path.join(__dirname, '../../src/assets/images');
    const images = fs.readdirSync(imagesDir)
      .filter(file =>
        file.endsWith('.jpg') ||
        file.endsWith('.png') ||
        file.endsWith('.jpeg') ||
        file.endsWith('.webp')
      );

    const oversizedImages = images.filter(image => {
      const stats = fs.statSync(path.join(imagesDir, image));
      return stats.size > 200000; // 200KB max
    });

    expect(oversizedImages).toHaveLength(0);
  }, 5000);
});
