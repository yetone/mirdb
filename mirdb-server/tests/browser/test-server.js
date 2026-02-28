/**
 * Simple static server for browser compatibility testing
 *
 * Serves the MirDB homepage assets and provides mock API endpoints.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 9999;
const ASSETS_DIR = path.join(__dirname, '../../assets');

const mimeTypes = {
    '.html': 'text/html',
    '.css': 'text/css',
    '.js': 'application/javascript',
    '.json': 'application/json',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.ico': 'image/x-icon',
};

// Mock API responses
const mockResponses = {
    '/api/status': {
        version: '1.0.0',
        status: 'running',
        running: true,
        uptime: 3600
    },
    '/api/config': {
        work_dir: '/var/lib/mirdb',
        addr: '127.0.0.1:12333',
        memtable_size_limit_formatted: '4 MB',
        sst_max_size_formatted: '256 MB',
        block_size_formatted: '4 KB',
        homepage_port: 9999
    },
    '/api/metrics': {
        active_connections: 42,
        total_keys: 12345,
        memtable_size: 2048576,
        sstable_levels: [
            { level: 0, file_count: 3, total_size: 1024000 },
            { level: 1, file_count: 5, total_size: 5120000 }
        ]
    }
};

const server = http.createServer((req, res) => {
    const url = req.url.split('?')[0];

    // Handle API endpoints
    if (mockResponses[url]) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(mockResponses[url]));
        return;
    }

    // Map URLs to file paths
    let filePath;
    if (url === '/') {
        filePath = path.join(ASSETS_DIR, 'index.html');
    } else if (url.startsWith('/static/')) {
        filePath = path.join(ASSETS_DIR, url.replace('/static/', ''));
    } else {
        filePath = path.join(ASSETS_DIR, url);
    }

    // Get file extension for MIME type
    const ext = path.extname(filePath).toLowerCase();
    const contentType = mimeTypes[ext] || 'application/octet-stream';

    // Serve the file
    fs.readFile(filePath, (err, content) => {
        if (err) {
            if (err.code === 'ENOENT') {
                res.writeHead(404);
                res.end('Not Found');
            } else {
                res.writeHead(500);
                res.end('Internal Server Error');
            }
            return;
        }

        res.writeHead(200, { 'Content-Type': contentType });
        res.end(content);
    });
});

server.listen(PORT, () => {
    console.log(`Test server running at http://localhost:${PORT}`);
    console.log(`Serving assets from: ${ASSETS_DIR}`);
});

// Handle shutdown gracefully
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
