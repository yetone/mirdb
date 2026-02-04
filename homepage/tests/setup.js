/**
 * Test Setup and Utilities
 * Owner: First builder (Shared)
 *
 * Expected exports:
 * - Test configuration
 * - Common test utilities
 * - Mock data
 * - Assertion helpers
 */

const path = require('path');
const fs = require('fs');

// Base URL for local testing
const BASE_URL = process.env.TEST_URL || 'http://localhost:1111';

// Paths
const HOMEPAGE_DIR = path.resolve(__dirname, '..');
const TEMPLATES_DIR = path.join(HOMEPAGE_DIR, 'templates');
const STATIC_DIR = path.join(HOMEPAGE_DIR, 'static');
const PUBLIC_DIR = path.join(HOMEPAGE_DIR, 'public');

// Test utilities
function readFile(filePath) {
    return fs.readFileSync(filePath, 'utf-8');
}

function fileExists(filePath) {
    return fs.existsSync(filePath);
}

// HTML parsing helpers
function extractMetaTags(html) {
    const metaTags = {};
    const metaRegex = /<meta\s+(?:name|property)=["']([^"']+)["']\s+content=["']([^"']+)["']/gi;
    let match;
    while ((match = metaRegex.exec(html)) !== null) {
        metaTags[match[1]] = match[2];
    }
    return metaTags;
}

function extractTitle(html) {
    const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
    return titleMatch ? titleMatch[1] : null;
}

function extractHeadings(html) {
    const headings = [];
    const headingRegex = /<h([1-6])[^>]*>([^<]+)<\/h\1>/gi;
    let match;
    while ((match = headingRegex.exec(html)) !== null) {
        headings.push({
            level: parseInt(match[1]),
            text: match[2].trim()
        });
    }
    return headings;
}

function extractCanonicalUrl(html) {
    const canonicalMatch = html.match(/<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i);
    return canonicalMatch ? canonicalMatch[1] : null;
}

function extractJsonLd(html) {
    const jsonLdMatch = html.match(/<script\s+type=["']application\/ld\+json["']>([^<]+)<\/script>/i);
    if (jsonLdMatch) {
        try {
            return JSON.parse(jsonLdMatch[1]);
        } catch (e) {
            return null;
        }
    }
    return null;
}

module.exports = {
    BASE_URL,
    HOMEPAGE_DIR,
    TEMPLATES_DIR,
    STATIC_DIR,
    PUBLIC_DIR,
    readFile,
    fileExists,
    extractMetaTags,
    extractTitle,
    extractHeadings,
    extractCanonicalUrl,
    extractJsonLd
};
