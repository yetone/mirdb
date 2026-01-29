/**
 * Astro Configuration.
 * Owner: First builder / Scenario 16
 *
 * Configuration:
 * - React integration for islands
 * - Tailwind integration
 * - Static output mode
 * - Site URL for SEO
 */
import { defineConfig } from 'astro/config';
import react from '@astrojs/react';
import tailwind from '@astrojs/tailwind';

// https://astro.build/config
export default defineConfig({
  integrations: [react(), tailwind()],
  output: 'static',
  site: 'https://mirdb.example.com',
});
