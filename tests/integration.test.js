/**
 * Integration Tests for Image Loading
 *
 * These tests verify that images can be served correctly
 * using a local HTTP server to simulate real browser loading.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

describe('Integration: Image Loading', () => {
  let server;
  let serverPort;

  // Simple static file server for testing
  function createServer() {
    return new Promise((resolve, reject) => {
      const server = http.createServer((req, res) => {
        const url = req.url === '/' ? '/index.html' : req.url;
        const filePath = path.join(__dirname, '..', url);

        fs.readFile(filePath, (err, data) => {
          if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
          }

          // Determine content type
          const ext = path.extname(filePath);
          const contentTypes = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.gif': 'image/gif',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.svg': 'image/svg+xml'
          };

          res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'application/octet-stream' });
          res.end(data);
        });
      });

      server.listen(0, () => {
        resolve({ server, port: server.address().port });
      });

      server.on('error', reject);
    });
  }

  beforeAll(async () => {
    const result = await createServer();
    server = result.server;
    serverPort = result.port;
  });

  afterAll((done) => {
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  function fetchUrl(urlPath) {
    return new Promise((resolve, reject) => {
      const req = http.get(`http://localhost:${serverPort}${urlPath}`, (res) => {
        let data = [];
        res.on('data', chunk => data.push(chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(data)
          });
        });
      });

      req.on('error', reject);
      req.setTimeout(5000, () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });
    });
  }

  describe('Test Case 3: All images load without 404 errors', () => {
    test('Logo image loads successfully (HTTP 200)', async () => {
      const response = await fetchUrl('/assets/logo.gif');
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toBe('image/gif');
      expect(response.body.length).toBeGreaterThan(0);
    });

    test('Usage GIF loads successfully (HTTP 200)', async () => {
      const response = await fetchUrl('/assets/usage.gif');
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toBe('image/gif');
      expect(response.body.length).toBeGreaterThan(0);
    });

    test('All images referenced in HTML return 200 status', async () => {
      // First, get the HTML and extract image sources
      const htmlPath = path.join(__dirname, '..', 'index.html');
      const htmlContent = fs.readFileSync(htmlPath, 'utf8');

      // Extract image sources using regex
      const imgSrcRegex = /<img[^>]+src=["']([^"']+)["']/g;
      const imageSources = [];
      let match;

      while ((match = imgSrcRegex.exec(htmlContent)) !== null) {
        const src = match[1];
        // Only test local images, not external URLs
        if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
          imageSources.push(src);
        }
      }

      expect(imageSources.length).toBeGreaterThan(0);

      // Test each image
      const results = await Promise.all(
        imageSources.map(async (src) => {
          try {
            const response = await fetchUrl('/' + src);
            return { src, statusCode: response.statusCode };
          } catch (error) {
            return { src, statusCode: 'error', error: error.message };
          }
        })
      );

      // Check all images returned 200
      const failedImages = results.filter(r => r.statusCode !== 200);
      if (failedImages.length > 0) {
        console.log('Failed to load images:', failedImages);
      }

      expect(failedImages.length).toBe(0);
    });
  });

  describe('Test Case 4: Usage demonstration accessibility', () => {
    test('Index page loads successfully', async () => {
      const response = await fetchUrl('/');
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toBe('text/html');
    });

    test('CSS file loads successfully', async () => {
      const response = await fetchUrl('/styles.css');
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toBe('text/css');
    });
  });
});
