/**
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Configuration Information Display', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  test('TC1: Page includes configuration or settings information section', () => {
    // Check for dedicated configuration section element
    const configSection = document.querySelector('#configuration') ||
                          document.querySelector('#config') ||
                          document.querySelector('.configuration') ||
                          document.querySelector('[data-section="configuration"]') ||
                          document.querySelector('section.config');

    expect(configSection).not.toBeNull();

    // Verify section has heading or content about configuration/defaults
    const sectionText = configSection.textContent.toLowerCase();
    const hasConfigInfo = sectionText.includes('configuration') ||
                          sectionText.includes('config') ||
                          sectionText.includes('default');
    expect(hasConfigInfo).toBe(true);
  });

  test('TC2: Configuration section shows default port 12333', () => {
    // Check for dedicated configuration section
    const configSection = document.querySelector('#configuration') ||
                          document.querySelector('#config') ||
                          document.querySelector('.configuration') ||
                          document.querySelector('[data-section="configuration"]');

    expect(configSection).not.toBeNull();

    // Verify port 12333 is mentioned in the configuration section
    const sectionText = configSection.textContent;
    expect(sectionText).toContain('12333');

    // Additionally verify it's labeled as a port or address
    const lowerText = sectionText.toLowerCase();
    const hasPortLabel = lowerText.includes('port') || lowerText.includes('addr');
    expect(hasPortLabel).toBe(true);
  });

  test('TC3: Configuration section mentions LSM levels, memtable size, or SSTable settings', () => {
    // Check for dedicated configuration section
    const configSection = document.querySelector('#configuration') ||
                          document.querySelector('#config') ||
                          document.querySelector('.configuration') ||
                          document.querySelector('[data-section="configuration"]');

    expect(configSection).not.toBeNull();

    const sectionText = configSection.textContent.toLowerCase();

    // Check for LSM/storage configuration mentions
    const hasLSMConfig = sectionText.includes('level') ||
                         sectionText.includes('memtable') ||
                         sectionText.includes('sstable') ||
                         sectionText.includes('mem_table') ||
                         sectionText.includes('sst');

    expect(hasLSMConfig).toBe(true);
  });
});
