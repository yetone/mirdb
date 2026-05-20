/**
 * Overview Section Layout Integration Tests
 * Owner: Scenario 2 - Overview Section
 *
 * Tests:
 * - Overview section layout within page flow
 * - Container alignment and spacing
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

describe('Overview Section Layout', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('overview section follows hero section in DOM order', () => {
    const sections = document.querySelectorAll('main > section');
    const sectionIds = Array.from(sections).map(s => s.id);

    const heroIndex = sectionIds.indexOf('hero');
    const overviewIndex = sectionIds.indexOf('overview');

    expect(heroIndex).toBeGreaterThanOrEqual(0);
    expect(overviewIndex).toBeGreaterThanOrEqual(0);
    expect(overviewIndex).toBeGreaterThan(heroIndex);
  });

  test('overview section has a container div', () => {
    const overview = document.querySelector('section#overview');
    const container = overview.querySelector('.container');
    expect(container).toBeTruthy();
  });

  test('overview container contains heading and paragraph', () => {
    const overview = document.querySelector('section#overview');
    const container = overview.querySelector('.container');

    expect(container.querySelector('h2')).toBeTruthy();
    expect(container.querySelector('p.overview-description')).toBeTruthy();
  });
});
