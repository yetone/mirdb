/**
 * Main Entry Point
 *
 * Initializes all scripts and components on the homepage.
 */

import { initCodeBlock } from '../components/QuickStart/CodeBlock.js';

document.addEventListener('DOMContentLoaded', () => {
  console.log('MirDB Homepage loaded');

  // Initialize code block functionality
  initCodeBlock();
});
