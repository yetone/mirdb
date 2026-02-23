/**
 * Simple static file server for E2E testing
 * Serves the MirDB homepage assets
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const ASSETS_DIR = path.join(__dirname, '../../mirdb-server/assets');
const ROOT_ASSETS = path.join(__dirname, '../../assets');

const MIME_TYPES = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.gif': 'image/gif',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
};

function getMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  return MIME_TYPES[ext] || 'application/octet-stream';
}

function serveFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  let urlPath = req.url.split('?')[0]; // Remove query string

  // Handle root path
  if (urlPath === '/') {
    urlPath = '/index.html';
  }

  // Handle assets from root assets folder (logo.gif)
  if (urlPath.startsWith('/assets/')) {
    const filePath = path.join(ROOT_ASSETS, urlPath.substring(8));
    if (fs.existsSync(filePath)) {
      serveFile(res, filePath, getMimeType(filePath));
      return;
    }
  }

  // Handle console API (mock for testing)
  if (urlPath === '/api/console' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
    });
    req.on('end', () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ message: 'STORED', command: body }));
    });
    return;
  }

  // Serve static files from assets directory
  const filePath = path.join(ASSETS_DIR, urlPath);

  // Security check: prevent directory traversal
  if (!filePath.startsWith(ASSETS_DIR) && !filePath.startsWith(ROOT_ASSETS)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    serveFile(res, filePath, getMimeType(filePath));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

server.listen(PORT, () => {
  console.log(`Static server running at http://localhost:${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  server.close(() => {
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  server.close(() => {
    process.exit(0);
  });
});
