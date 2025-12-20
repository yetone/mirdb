/**
 * @jest-environment jsdom
 */

const fs = require('fs');
const path = require('path');

describe('Supported Commands Display', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    document = new DOMParser().parseFromString(html, 'text/html');
  });

  test('TC1: Page contains section for supported commands', () => {
    // Check for commands section element
    const commandsSection = document.querySelector('#commands') ||
                            document.querySelector('#supported-commands') ||
                            document.querySelector('.commands') ||
                            document.querySelector('[data-section="commands"]') ||
                            document.querySelector('section.commands');

    expect(commandsSection).not.toBeNull();

    // Check that section contains heading about commands
    const sectionText = commandsSection.textContent.toLowerCase();
    expect(sectionText).toMatch(/command/i);
  });

  test('TC2: Commands section includes SET', () => {
    const commandsSection = document.querySelector('#commands') ||
                            document.querySelector('#supported-commands') ||
                            document.querySelector('.commands') ||
                            document.querySelector('[data-section="commands"]');

    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent.toUpperCase();
    expect(sectionText).toContain('SET');
  });

  test('TC3: Commands section includes GET', () => {
    const commandsSection = document.querySelector('#commands') ||
                            document.querySelector('#supported-commands') ||
                            document.querySelector('.commands') ||
                            document.querySelector('[data-section="commands"]');

    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent.toUpperCase();
    expect(sectionText).toContain('GET');
  });

  test('TC4: Commands section includes DELETE', () => {
    const commandsSection = document.querySelector('#commands') ||
                            document.querySelector('#supported-commands') ||
                            document.querySelector('.commands') ||
                            document.querySelector('[data-section="commands"]');

    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent.toUpperCase();
    expect(sectionText).toContain('DELETE');
  });

  test('TC5: Commands section includes ADD, REPLACE, APPEND, or PREPEND', () => {
    const commandsSection = document.querySelector('#commands') ||
                            document.querySelector('#supported-commands') ||
                            document.querySelector('.commands') ||
                            document.querySelector('[data-section="commands"]');

    expect(commandsSection).not.toBeNull();

    const sectionText = commandsSection.textContent.toUpperCase();
    const hasAdditionalStorageCommand = sectionText.includes('ADD') ||
                                        sectionText.includes('REPLACE') ||
                                        sectionText.includes('APPEND') ||
                                        sectionText.includes('PREPEND');

    expect(hasAdditionalStorageCommand).toBe(true);
  });
});
