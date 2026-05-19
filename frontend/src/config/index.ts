/**
 * Environment-driven configuration.
 */

export const config = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || '/api',
  appName: 'URL Shortener',
  isDevelopment: import.meta.env.DEV,
  isProduction: import.meta.env.PROD,
};
