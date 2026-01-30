/**
 * Features Section Component.
 * Owner: Scenario 2 - Features Section Grid Display
 *
 * Displays a responsive grid of feature cards:
 * 1. Persistent Storage
 * 2. Memcached Protocol
 * 3. LSM-Tree Architecture
 * 4. Compaction
 * 5. Tokio Async
 * 6. Rust Performance
 *
 * Each card has: icon, title, description
 *
 * Expected exports:
 * - renderFeatures(): HTMLElement
 * - FEATURES: Feature[]
 */

import type { Feature } from '../types/index';

/**
 * Array of feature data for MirDB
 */
export const FEATURES: Feature[] = [
  {
    id: 'persistent-storage',
    title: 'Persistent Storage',
    description: 'Data persists to disk, surviving restarts and power failures. Unlike in-memory caches, your data is safe and durable.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
    </svg>`,
  },
  {
    id: 'memcached-protocol',
    title: 'Memcached Protocol',
    description: 'Full memcached protocol compatibility means you can use existing clients and tools without any code changes.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
    </svg>`,
  },
  {
    id: 'lsm-tree-architecture',
    title: 'LSM-Tree Architecture',
    description: 'Built on a Log-Structured Merge-tree storage engine for optimal write performance and efficient disk utilization.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
    </svg>`,
  },
  {
    id: 'compaction',
    title: 'Compaction',
    description: 'Automatic minor and major compaction processes keep storage efficient by merging and cleaning up data files.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 7.5L7.5 3m0 0L12 7.5M7.5 3v13.5m13.5 0L16.5 21m0 0L12 16.5m4.5 4.5V7.5" />
    </svg>`,
  },
  {
    id: 'tokio-async',
    title: 'Tokio Async',
    description: 'Powered by Tokio async runtime for high-performance, non-blocking network I/O and concurrent request handling.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
    </svg>`,
  },
  {
    id: 'rust-performance',
    title: 'Rust Performance',
    description: 'Written in Rust for memory safety and blazing fast performance with zero-cost abstractions.',
    icon: `<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor" class="w-12 h-12">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15.59 14.37a6 6 0 01-5.84 7.38v-4.8m5.84-2.58a14.98 14.98 0 006.16-12.12A14.98 14.98 0 009.631 8.41m5.96 5.96a14.926 14.926 0 01-5.841 2.58m-.119-8.54a6 6 0 00-7.381 5.84h4.8m2.581-5.84a14.927 14.927 0 00-2.58 5.84m2.699 2.7c-.103.021-.207.041-.311.06a15.09 15.09 0 01-2.448-2.448 14.9 14.9 0 01.06-.312m-2.24 2.39a4.493 4.493 0 00-1.757 4.306 4.493 4.493 0 004.306-1.758M16.5 9a1.5 1.5 0 11-3 0 1.5 1.5 0 013 0z" />
    </svg>`,
  },
];

/**
 * Creates a feature card element
 */
function createFeatureCard(feature: Feature): HTMLElement {
  const card = document.createElement('div');
  card.className = 'feature-card bg-white rounded-lg shadow-md p-6 transition-transform duration-300 hover:transform hover:-translate-y-1';
  card.setAttribute('data-testid', `feature-${feature.id}`);
  card.classList.add('feature-card-item');

  const iconContainer = document.createElement('div');
  iconContainer.className = 'feature-icon w-12 h-12 mb-4 text-blue-500';
  iconContainer.setAttribute('data-testid', 'feature-icon');
  iconContainer.innerHTML = feature.icon;

  const title = document.createElement('h3');
  title.className = 'feature-title text-xl font-semibold text-gray-900 mb-2';
  title.setAttribute('data-testid', 'feature-title');
  title.textContent = feature.title;

  const description = document.createElement('p');
  description.className = 'feature-description text-gray-600 leading-relaxed';
  description.setAttribute('data-testid', 'feature-description');
  description.textContent = feature.description;

  card.appendChild(iconContainer);
  card.appendChild(title);
  card.appendChild(description);

  return card;
}

/**
 * Renders the features section
 * @returns HTMLElement - The features section element
 */
export function renderFeatures(): HTMLElement {
  const section = document.createElement('section');
  section.className = 'features-section py-16 px-4 bg-gray-50';
  section.id = 'features';

  const container = document.createElement('div');
  container.className = 'features-container max-w-6xl mx-auto';

  const sectionTitle = document.createElement('h2');
  sectionTitle.className = 'features-title text-3xl font-bold text-center text-gray-900 mb-12';
  sectionTitle.textContent = 'Key Features';

  const grid = document.createElement('div');
  grid.className = 'features-grid grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8';
  grid.setAttribute('data-testid', 'features-grid');

  // Add feature cards to grid
  FEATURES.forEach((feature) => {
    const card = createFeatureCard(feature);
    grid.appendChild(card);
  });

  container.appendChild(sectionTitle);
  container.appendChild(grid);
  section.appendChild(container);

  return section;
}
