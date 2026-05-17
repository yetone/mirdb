import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import Hero from '../../../src/components/homepage/Hero';

describe('Hero', () => {
  const renderHero = () =>
    render(
      <MemoryRouter>
        <Hero />
      </MemoryRouter>
    );

  it('renders an h1 heading containing the MirDB product name', () => {
    renderHero();

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).toMatch(/mirdb/i);
  });

  it('renders a tagline mentioning URL and analytics/tracking', () => {
    renderHero();

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    const text = tagline.textContent ?? '';
    expect(text).toMatch(/url|short/i);
    expect(text).toMatch(/analytics|tracking/i);
  });

  it('exposes a hero region via data-testid="hero"', () => {
    renderHero();

    const hero = screen.getByTestId('hero');
    expect(hero).toBeInTheDocument();
    expect(hero.tagName.toLowerCase()).toBe('section');
  });

  it('hero region contains the h1 product name, tagline, and at least one paragraph', () => {
    renderHero();

    const hero = screen.getByTestId('hero');

    const heading = within(hero).getByRole('heading', { level: 1 });
    expect(heading.textContent).toMatch(/mirdb/i);

    const tagline = within(hero).getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();

    const paragraphs = hero.querySelectorAll('p');
    expect(paragraphs.length).toBeGreaterThanOrEqual(1);

    const valueProp = within(hero).getByTestId('hero-value-prop');
    expect(valueProp).toBeInTheDocument();
    expect((valueProp.textContent ?? '').length).toBeGreaterThan(20);
  });

  it('hero section is accessibly labelled by its heading', () => {
    renderHero();

    const hero = screen.getByTestId('hero');
    const heading = within(hero).getByRole('heading', { level: 1 });
    const labelledBy = hero.getAttribute('aria-labelledby');
    expect(labelledBy).toBeTruthy();
    expect(heading.id).toBe(labelledBy);
  });

  it('renders gracefully without a ThemeProvider', () => {
    expect(() => renderHero()).not.toThrow();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toMatch(/mirdb/i);
  });
});
