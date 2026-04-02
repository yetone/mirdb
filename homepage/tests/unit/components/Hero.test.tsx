/**
 * Unit tests for Hero component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test Case 5: Verify hero section renders without errors with all required props
 */
import React from 'react';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Hero } from '@/components/sections/Hero';

// Mock next/link since we're in a unit test environment
jest.mock('next/link', () => {
  return function MockLink({ children, href }: { children: React.ReactNode; href: string }) {
    return <a href={href}>{children}</a>;
  };
});

describe('Hero Component', () => {
  test('TC5: Hero component renders with all required props', () => {
    render(<Hero />);

    // Verify the hero section renders
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Verify logo is present
    const logo = screen.getByTestId('hero-logo');
    expect(logo).toBeInTheDocument();

    // Verify headline is present and has correct content
    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent('MirDB');

    // Verify tagline is present
    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent(/memcached/i);
    expect(tagline).toHaveTextContent(/persistence/i);

    // Verify CTA button is present
    const cta = screen.getByTestId('hero-cta');
    expect(cta).toBeInTheDocument();
    expect(cta).toHaveTextContent('Get Started');
  });

  test('Hero component accepts custom props', () => {
    const customProps = {
      title: 'Custom Title: Key-Value Store',
      tagline: 'Custom tagline with Memcached protocol and persistence',
      ctaText: 'Custom CTA',
      ctaHref: 'https://example.com',
    };

    render(<Hero {...customProps} />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toHaveTextContent(customProps.title);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toHaveTextContent(customProps.tagline);

    const cta = screen.getByTestId('hero-cta');
    expect(cta).toHaveTextContent(customProps.ctaText);
    expect(cta).toHaveAttribute('href', customProps.ctaHref);
  });

  test('Hero component renders with default values from constants', () => {
    render(<Hero />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toHaveTextContent('MirDB: Persistent Key-Value Store');

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toHaveTextContent('Memcached protocol with disk persistence');

    const cta = screen.getByTestId('hero-cta');
    expect(cta).toHaveTextContent('Get Started');
    expect(cta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });
});
