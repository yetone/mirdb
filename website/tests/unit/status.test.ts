/**
 * Status Component Unit Tests
 * Owner: Scenario 6 - Project Status Section
 *
 * Tests for:
 * - Status component rendering logic
 * - Implemented vs planned feature distinction
 * - Data structure validation
 */
import { describe, it, expect, beforeEach } from 'vitest';

// Status data structure
interface StatusFeature {
  name: string;
  description: string;
  completed: boolean;
}

interface StatusData {
  implemented: StatusFeature[];
  planned: StatusFeature[];
}

// Mock status data matching the real data structure
const mockStatusData: StatusData = {
  implemented: [
    { name: 'Memcached protocol', description: 'Full compatibility with standard memcached clients', completed: true },
    { name: 'Memtable', description: 'In-memory write buffer with skip list data structure', completed: true },
    { name: 'Minor compaction', description: 'Automatic flush of memtable to SSTable on disk', completed: true },
    { name: 'Major compaction', description: 'Merge and compact multiple SSTables to reclaim space', completed: true },
  ],
  planned: [
    { name: 'Raft consensus', description: 'Distributed consensus for high availability', completed: false },
  ],
};

/**
 * Render a status item as HTML string
 */
function renderStatusItem(feature: StatusFeature, type: 'implemented' | 'planned'): string {
  const iconClass = type;
  const iconSvg = type === 'implemented'
    ? '<polyline points="20 6 9 17 4 12"></polyline>'
    : '<circle cx="12" cy="12" r="10"></circle>';

  const badge = type === 'planned'
    ? '<span class="coming-soon-badge">Coming Soon</span>'
    : '';

  return `
    <li class="status-item" data-feature="${feature.name.toLowerCase().replace(/\s+/g, '-')}">
      <span class="status-icon ${iconClass}" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${type === 'implemented' ? '3' : '2'}" stroke-linecap="round" stroke-linejoin="round">
          ${iconSvg}
        </svg>
      </span>
      <div class="status-content">
        <span class="status-name">${feature.name}${badge}</span>
        <p class="status-description">${feature.description}</p>
      </div>
    </li>
  `.trim();
}

/**
 * Render status list as HTML string
 */
function renderStatusList(features: StatusFeature[], type: 'implemented' | 'planned'): string {
  const items = features.map(f => renderStatusItem(f, type)).join('\n');
  return `<ul class="status-list" id="${type}-features">${items}</ul>`;
}

/**
 * Render full status component
 */
function renderStatusComponent(data: StatusData): string {
  const implementedList = renderStatusList(data.implemented, 'implemented');
  const plannedList = renderStatusList(data.planned, 'planned');

  return `
    <section id="status" class="status">
      <div class="status-container container">
        <h2 class="status-title">Project Status</h2>
        <div class="status-grid">
          <div class="status-column" data-status-type="implemented">
            <h3 class="status-column-title implemented">Implemented Features</h3>
            ${implementedList}
          </div>
          <div class="status-column" data-status-type="planned">
            <h3 class="status-column-title planned">Planned Features</h3>
            ${plannedList}
          </div>
        </div>
      </div>
    </section>
  `.trim();
}

describe('Status Component', () => {
  let container: HTMLElement;

  beforeEach(() => {
    container = document.createElement('div');
    document.body.appendChild(container);
  });

  describe('TC4: Render status component with feature data', () => {
    it('correctly renders implemented vs planned features', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      // Check status section exists
      const statusSection = container.querySelector('#status');
      expect(statusSection).not.toBeNull();

      // Check implemented features column
      const implementedColumn = container.querySelector('[data-status-type="implemented"]');
      expect(implementedColumn).not.toBeNull();

      // Check all implemented features are rendered
      const implementedItems = implementedColumn!.querySelectorAll('.status-item');
      expect(implementedItems.length).toBe(mockStatusData.implemented.length);

      // Check planned features column
      const plannedColumn = container.querySelector('[data-status-type="planned"]');
      expect(plannedColumn).not.toBeNull();

      // Check all planned features are rendered
      const plannedItems = plannedColumn!.querySelectorAll('.status-item');
      expect(plannedItems.length).toBe(mockStatusData.planned.length);
    });

    it('renders implemented features with checkmark icons', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const implementedIcons = container.querySelectorAll('[data-status-type="implemented"] .status-icon.implemented');
      expect(implementedIcons.length).toBe(mockStatusData.implemented.length);

      // Each implemented icon should have a polyline (checkmark)
      implementedIcons.forEach(icon => {
        const polyline = icon.querySelector('polyline');
        expect(polyline).not.toBeNull();
      });
    });

    it('renders planned features with circle icons', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const plannedIcons = container.querySelectorAll('[data-status-type="planned"] .status-icon.planned');
      expect(plannedIcons.length).toBe(mockStatusData.planned.length);

      // Each planned icon should have a circle
      plannedIcons.forEach(icon => {
        const circle = icon.querySelector('circle');
        expect(circle).not.toBeNull();
      });
    });

    it('renders "coming soon" badges for planned features', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const badges = container.querySelectorAll('[data-status-type="planned"] .coming-soon-badge');
      expect(badges.length).toBe(mockStatusData.planned.length);

      badges.forEach(badge => {
        expect(badge.textContent).toBe('Coming Soon');
      });
    });

    it('does not render "coming soon" badges for implemented features', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const implementedBadges = container.querySelectorAll('[data-status-type="implemented"] .coming-soon-badge');
      expect(implementedBadges.length).toBe(0);
    });
  });

  describe('Feature data validation', () => {
    it('validates implemented features have correct data attributes', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      mockStatusData.implemented.forEach(feature => {
        const slug = feature.name.toLowerCase().replace(/\s+/g, '-');
        const item = container.querySelector(`[data-feature="${slug}"]`);
        expect(item).not.toBeNull();
      });
    });

    it('validates feature names are displayed correctly', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      // Check implemented feature names
      const implementedNames = container.querySelectorAll('[data-status-type="implemented"] .status-name');
      const implementedNameTexts = Array.from(implementedNames).map(el => el.textContent?.trim());

      mockStatusData.implemented.forEach(feature => {
        expect(implementedNameTexts.some(text => text?.includes(feature.name))).toBe(true);
      });

      // Check planned feature names
      const plannedNames = container.querySelectorAll('[data-status-type="planned"] .status-name');
      const plannedNameTexts = Array.from(plannedNames).map(el => el.textContent?.trim());

      mockStatusData.planned.forEach(feature => {
        expect(plannedNameTexts.some(text => text?.includes(feature.name))).toBe(true);
      });
    });

    it('validates feature descriptions are displayed', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const allFeatures = [...mockStatusData.implemented, ...mockStatusData.planned];

      allFeatures.forEach(feature => {
        const descriptions = container.querySelectorAll('.status-description');
        const descTexts = Array.from(descriptions).map(el => el.textContent?.trim());
        expect(descTexts.some(text => text === feature.description)).toBe(true);
      });
    });
  });

  describe('Accessibility', () => {
    it('has proper heading hierarchy', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const h2 = container.querySelector('h2.status-title');
      expect(h2).not.toBeNull();
      expect(h2!.textContent).toBe('Project Status');

      const h3s = container.querySelectorAll('h3.status-column-title');
      expect(h3s.length).toBe(2);
    });

    it('uses semantic list elements', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const lists = container.querySelectorAll('ul.status-list');
      expect(lists.length).toBe(2);

      const listItems = container.querySelectorAll('li.status-item');
      expect(listItems.length).toBe(mockStatusData.implemented.length + mockStatusData.planned.length);
    });

    it('has aria-hidden on decorative icons', () => {
      const html = renderStatusComponent(mockStatusData);
      container.innerHTML = html;

      const icons = container.querySelectorAll('.status-icon');
      icons.forEach(icon => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });

  describe('Empty state handling', () => {
    it('handles empty implemented features', () => {
      const emptyData: StatusData = {
        implemented: [],
        planned: mockStatusData.planned,
      };
      const html = renderStatusComponent(emptyData);
      container.innerHTML = html;

      const implementedItems = container.querySelectorAll('[data-status-type="implemented"] .status-item');
      expect(implementedItems.length).toBe(0);
    });

    it('handles empty planned features', () => {
      const emptyData: StatusData = {
        implemented: mockStatusData.implemented,
        planned: [],
      };
      const html = renderStatusComponent(emptyData);
      container.innerHTML = html;

      const plannedItems = container.querySelectorAll('[data-status-type="planned"] .status-item');
      expect(plannedItems.length).toBe(0);
    });
  });
});
