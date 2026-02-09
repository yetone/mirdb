/**
 * Features Section Component.
 * Owner: Scenario 2 - Feature Overview Section
 *
 * Requirements: REQ-2
 *
 * Renders a grid of feature cards displaying MirDB's key capabilities.
 */

import { features } from '../data/features';
import type { Feature } from '../types';

function createFeatureCard(feature: Feature): HTMLElement {
  const card = document.createElement('article');
  card.className = 'feature-card';

  const icon = document.createElement('span');
  icon.className = 'feature-icon';
  icon.textContent = feature.icon;
  icon.setAttribute('aria-hidden', 'true');

  const title = document.createElement('h3');
  title.className = 'feature-title';
  title.textContent = feature.title;

  const description = document.createElement('p');
  description.className = 'feature-description';
  description.textContent = feature.description;

  card.appendChild(icon);
  card.appendChild(title);
  card.appendChild(description);

  return card;
}

export function renderFeatures(): HTMLElement {
  const section = document.createElement('section');
  section.id = 'features';
  section.className = 'features-section';
  section.setAttribute('aria-labelledby', 'features-heading');

  const container = document.createElement('div');
  container.className = 'container';

  const heading = document.createElement('h2');
  heading.id = 'features-heading';
  heading.textContent = 'Key Features';

  const grid = document.createElement('div');
  grid.className = 'features-grid';

  features.forEach((feature) => {
    const card = createFeatureCard(feature);
    grid.appendChild(card);
  });

  container.appendChild(heading);
  container.appendChild(grid);
  section.appendChild(container);

  return section;
}
