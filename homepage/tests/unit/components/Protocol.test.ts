/**
 * Unit tests for Protocol component.
 * Owner: Scenario 5 - Protocol Documentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { renderProtocol } from '../../../src/components/Protocol';
import { commands } from '../../../src/data/commands';

describe('Protocol Component', () => {
  let section: HTMLElement;

  beforeEach(() => {
    section = renderProtocol();
  });

  it('renders a section element with proper id', () => {
    expect(section.tagName).toBe('SECTION');
    expect(section.id).toBe('protocol');
    expect(section.className).toContain('protocol-section');
  });

  it('has proper heading structure', () => {
    const heading = section.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading?.id).toBe('protocol-heading');
    expect(heading?.textContent).toBe('Protocol Documentation');
  });

  it('has accessible aria attributes', () => {
    expect(section.getAttribute('aria-labelledby')).toBe('protocol-heading');
  });

  it('renders an introduction paragraph', () => {
    const intro = section.querySelector('.protocol-intro');
    expect(intro).not.toBeNull();
    expect(intro?.textContent).toContain('Memcached text protocol');
  });

  it('renders all commands in a grid', () => {
    const grid = section.querySelector('.commands-grid');
    expect(grid).not.toBeNull();
    expect(grid?.children.length).toBe(commands.length);
  });

  it('renders SET command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const setCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'SET'
    );
    expect(setCard).not.toBeNull();

    const syntax = setCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('set');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders GET command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const getCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'GET'
    );
    expect(getCard).not.toBeNull();

    const syntax = getCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('get');
    expect(syntax?.textContent).toContain('<key');
  });

  it('renders DELETE command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const deleteCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'DELETE'
    );
    expect(deleteCard).not.toBeNull();

    const syntax = deleteCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('delete');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders ADD command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const addCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'ADD'
    );
    expect(addCard).not.toBeNull();

    const syntax = addCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('add');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders REPLACE command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const replaceCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'REPLACE'
    );
    expect(replaceCard).not.toBeNull();

    const syntax = replaceCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('replace');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders APPEND command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const appendCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'APPEND'
    );
    expect(appendCard).not.toBeNull();

    const syntax = appendCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('append');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders PREPEND command with syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const prependCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'PREPEND'
    );
    expect(prependCard).not.toBeNull();

    const syntax = prependCard?.querySelector('.command-syntax');
    expect(syntax).not.toBeNull();
    expect(syntax?.textContent).toContain('prepend');
    expect(syntax?.textContent).toContain('<key>');
  });

  it('renders INFO command as MirDB-specific', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const infoCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'INFO'
    );
    expect(infoCard).not.toBeNull();
    expect(infoCard?.classList.contains('mirdb-specific')).toBe(true);

    const badge = infoCard?.querySelector('.mirdb-badge');
    expect(badge).not.toBeNull();
    expect(badge?.textContent).toBe('MirDB');
  });

  it('renders MAJOR_COMPACTION command as MirDB-specific', () => {
    const commandCards = section.querySelectorAll('.command-card');
    const compactionCard = Array.from(commandCards).find((card) =>
      card.querySelector('.command-name')?.textContent === 'MAJOR_COMPACTION'
    );
    expect(compactionCard).not.toBeNull();
    expect(compactionCard?.classList.contains('mirdb-specific')).toBe(true);

    const badge = compactionCard?.querySelector('.mirdb-badge');
    expect(badge).not.toBeNull();
    expect(badge?.textContent).toBe('MirDB');
  });

  it('MirDB-specific commands are visually distinguished with badge and special class', () => {
    const mirdbCards = section.querySelectorAll('.command-card.mirdb-specific');
    expect(mirdbCards.length).toBe(2); // INFO and MAJOR_COMPACTION

    mirdbCards.forEach((card) => {
      const badge = card.querySelector('.mirdb-badge');
      expect(badge).not.toBeNull();
      expect(badge?.textContent).toBe('MirDB');
    });
  });

  it('each command card has name, description, and syntax', () => {
    const commandCards = section.querySelectorAll('.command-card');

    commandCards.forEach((card) => {
      const name = card.querySelector('.command-name');
      const description = card.querySelector('.command-description');
      const syntax = card.querySelector('.command-syntax');

      expect(name).not.toBeNull();
      expect(description).not.toBeNull();
      expect(syntax).not.toBeNull();

      expect(name?.textContent?.trim().length).toBeGreaterThan(0);
      expect(description?.textContent?.trim().length).toBeGreaterThan(0);
      expect(syntax?.textContent?.trim().length).toBeGreaterThan(0);
    });
  });

  it('uses semantic article elements for command cards', () => {
    const cards = section.querySelectorAll('.command-card');
    cards.forEach((card) => {
      expect(card.tagName).toBe('ARTICLE');
    });
  });

  it('standard commands do not have MirDB badge', () => {
    const standardCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
    const commandCards = section.querySelectorAll('.command-card');

    standardCommands.forEach((commandName) => {
      const card = Array.from(commandCards).find((c) =>
        c.querySelector('.command-name')?.textContent === commandName
      );
      expect(card).not.toBeNull();
      expect(card?.classList.contains('mirdb-specific')).toBe(false);
      expect(card?.querySelector('.mirdb-badge')).toBeNull();
    });
  });
});
