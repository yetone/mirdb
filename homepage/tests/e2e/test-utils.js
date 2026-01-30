/**
 * MirDB Homepage E2E Tests - Shared Utilities
 * Created by first E2E test builder
 *
 * Shared utilities for Playwright tests:
 * - Page navigation helpers
 * - Element selectors
 * - Viewport configuration
 * - Common assertions
 */

// Test configuration
const BASE_URL = process.env.BASE_URL || 'http://localhost:3000';

// Viewport configurations
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1280, height: 800 }
};

// Common selectors
const SELECTORS = {
  header: '#header',
  hero: '#hero',
  features: '#features',
  architecture: '#architecture',
  usage: '#usage',
  status: '#status',
  gettingStarted: '#getting-started',
  footer: '#footer'
};

module.exports = {
  BASE_URL,
  VIEWPORTS,
  SELECTORS
};
