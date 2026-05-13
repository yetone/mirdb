/**
 * Unit tests for Docs component.
 * Owner: Scenario 5 - Documentation Links
 *
 * Test cases from scenarios.json id=5:
 * - Renders at least 3 documentation cards
 * - Each card has heading, description, and link
 * - Valid href attributes
 * - Cards cover README, API docs, technical docs
 * - Hover effects (Tailwind hover classes)
 * - Keyboard focusable with visible focus indicators
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import Docs from '../../src/components/Docs';

describe('Docs component', () => {
  it('renders the documentation section with label', () => {
    render(<Docs />);
    const section = screen.getByRole('region', { name: /documentation/i });
    expect(section).toBeInTheDocument();
  });

  it('renders at least 3 documentation cards', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    expect(cards.length).toBeGreaterThanOrEqual(3);
  });

  it('each card has a heading, description text, and a link element', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      const heading = card.querySelector('h3');
      expect(heading).not.toBeNull();
      expect(heading!.textContent).toBeTruthy();

      const description = card.querySelector('p');
      expect(description).not.toBeNull();
      expect(description!.textContent).toBeTruthy();

      expect(card.getAttribute('href')).toBeTruthy();
    }
  });

  it('each card link has a valid, non-empty href attribute', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      const href = card.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href!.trim().length).toBeGreaterThan(0);
    }
  });

  it('includes a card linking to the project README/GitHub', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    const readmeCard = cards.find(
      (card) =>
        card.getAttribute('href')?.includes('github.com/yetone/mirdb') ||
        card.textContent?.toLowerCase().includes('readme') ||
        card.textContent?.toLowerCase().includes('project overview'),
    );
    expect(readmeCard).toBeTruthy();
  });

  it('includes a card linking to API/protocol documentation', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    const apiCard = cards.find(
      (card) =>
        card.getAttribute('href')?.includes('protocol.txt') ||
        card.textContent?.toLowerCase().includes('api') ||
        card.textContent?.toLowerCase().includes('protocol'),
    );
    expect(apiCard).toBeTruthy();
  });

  it('includes a card linking to technical architecture documentation', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    const techCard = cards.find(
      (card) =>
        card.textContent?.toLowerCase().includes('architecture') ||
        card.textContent?.toLowerCase().includes('technical'),
    );
    expect(techCard).toBeTruthy();
  });

  it('each card has hover classes for interactivity', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      const classes = card.className;
      const hasHoverClass =
        classes.includes('hover:border') ||
        classes.includes('hover:shadow') ||
        classes.includes('group-hover:');
      expect(hasHoverClass).toBe(true);
    }
  });

  it('cards are keyboard focusable with focus-visible outlines', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      const classes = card.className;
      const hasFocusClass =
        classes.includes('focus-visible:outline') ||
        classes.includes('focus-visible:border') ||
        classes.includes('focus-visible:shadow');
      expect(hasFocusClass).toBe(true);
    }
  });

  it('cards open external links in new tabs', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      expect(card.getAttribute('target')).toBe('_blank');
      expect(card.getAttribute('rel')).toContain('noopener');
      expect(card.getAttribute('rel')).toContain('noreferrer');
    }
  });

  it('cards include arrow icon indicating link destination', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');

    for (const card of cards) {
      const hasArrowIndicator = card.querySelector('svg');
      expect(hasArrowIndicator).not.toBeNull();
    }
  });

  it('renders the expected heading for main documentation section', () => {
    render(<Docs />);
    expect(screen.getByText('Documentation')).toBeInTheDocument();
  });
});
