/**
 * Commands Section Unit Tests
 * Owner: Scenario 7 - Supported Commands List
 *
 * Tests:
 * - Commands section exists
 * - GET command is listed
 * - SET command is listed
 * - DELETE command is listed
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('Supported Commands List (Unit)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC2: Verify GET command is listed', () => {
    // Find the commands section
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    // Look for GET command card
    const getCommandCard = commandsSection.querySelector('[data-command="get"]');
    expect(getCommandCard).not.toBeNull();

    // Verify GET command code is displayed
    const getCommandCode = getCommandCard.querySelector('.commands-code');
    expect(getCommandCode).not.toBeNull();
    expect(getCommandCode.textContent).toBe('GET');

    // Verify description exists
    const description = getCommandCard.querySelector('p');
    expect(description).not.toBeNull();
    expect(description.textContent.toLowerCase()).toContain('retrieve');
  });

  it('TC3: Verify SET command is listed', () => {
    // Find the commands section
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    // Look for SET command card
    const setCommandCard = commandsSection.querySelector('[data-command="set"]');
    expect(setCommandCard).not.toBeNull();

    // Verify SET command code is displayed
    const setCommandCode = setCommandCard.querySelector('.commands-code');
    expect(setCommandCode).not.toBeNull();
    expect(setCommandCode.textContent).toBe('SET');

    // Verify description exists
    const description = setCommandCard.querySelector('p');
    expect(description).not.toBeNull();
    expect(description.textContent.toLowerCase()).toContain('store');
  });

  it('TC4: Verify DELETE command is listed', () => {
    // Find the commands section
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    // Look for DELETE command card
    const deleteCommandCard = commandsSection.querySelector('[data-command="delete"]');
    expect(deleteCommandCard).not.toBeNull();

    // Verify DELETE command code is displayed
    const deleteCommandCode = deleteCommandCard.querySelector('.commands-code');
    expect(deleteCommandCode).not.toBeNull();
    expect(deleteCommandCode.textContent).toBe('DELETE');

    // Verify description exists
    const description = deleteCommandCard.querySelector('p');
    expect(description).not.toBeNull();
    expect(description.textContent.toLowerCase()).toContain('remove');
  });

  it('Commands section has proper heading', () => {
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    const heading = commandsSection.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent).toContain('Supported Commands');
  });

  it('All required memcached commands are present', () => {
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    // Check for all supported commands
    const requiredCommands = ['get', 'gets', 'set', 'add', 'replace', 'delete', 'append', 'prepend', 'info'];

    requiredCommands.forEach(command => {
      const commandCard = commandsSection.querySelector(`[data-command="${command}"]`);
      expect(commandCard, `Command ${command} should be present`).not.toBeNull();
    });
  });

  it('Commands have syntax examples', () => {
    const commandsSection = document.querySelector('#commands');
    expect(commandsSection).not.toBeNull();

    // Each command card should have a pre/code block with syntax
    const commandCards = commandsSection.querySelectorAll('.commands-card');
    expect(commandCards.length).toBeGreaterThan(0);

    commandCards.forEach(card => {
      const syntaxBlock = card.querySelector('pre code');
      expect(syntaxBlock, 'Each command card should have syntax example').not.toBeNull();
    });
  });
});
