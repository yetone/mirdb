import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import Home from '../../src/pages/Home';

describe('Home page', () => {
  const renderHome = () =>
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    );

  it('renders an h1 with the MirDB product name (REQ-1)', () => {
    renderHome();

    const heading = screen.getByRole('heading', { level: 1, name: /mirdb/i });
    expect(heading).toBeInTheDocument();
  });

  it('renders a tagline describing URL shortening with analytics/tracking (REQ-1)', () => {
    renderHome();

    const tagline = screen.getByTestId('hero-tagline');
    const text = tagline.textContent ?? '';
    expect(text).toMatch(/url|short/i);
    expect(text).toMatch(/analytics|tracking/i);
  });

  it('renders the hero region with brand, tagline, and a value-proposition paragraph (REQ-2)', () => {
    renderHome();

    const hero = screen.getByTestId('hero');
    expect(hero).toBeInTheDocument();

    const heading = within(hero).getByRole('heading', { level: 1 });
    expect(heading.textContent).toMatch(/mirdb/i);

    expect(within(hero).getByTestId('hero-tagline')).toBeInTheDocument();

    const paragraphs = hero.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(1);
  });

  it('sets document.title to include the brand and "URL Shortening" (US-1)', async () => {
    renderHome();

    await screen.findByRole('heading', { level: 1, name: /mirdb/i });
    expect(document.title).toMatch(/mirdb/i);
    expect(document.title).toMatch(/url shortening/i);
    expect(document.title).toBe('MirDB - URL Shortening Service');
  });

  it('renders gracefully without a ThemeContext provider, keeping the brand visible', () => {
    expect(() => renderHome()).not.toThrow();

    const heading = screen.getByRole('heading', { level: 1, name: /mirdb/i });
    expect(heading).toBeInTheDocument();
    expect(heading).toBeVisible();
  });

  it('wraps content in a <main> region with id="main-content" for skip-to-content', () => {
    renderHome();

    const main = screen.getByRole('main');
    expect(main).toHaveAttribute('id', 'main-content');
  });

  it('renders the hero before the features section in DOM order', () => {
    renderHome();

    const main = screen.getByRole('main');
    const hero = within(main).getByTestId('hero');
    const sections = main.querySelectorAll('section');
    expect(sections[0]).toBe(hero);
  });
});
