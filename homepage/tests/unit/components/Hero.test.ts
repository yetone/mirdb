/**
 * Hero Section Component Tests
 * Owner: Scenario 1 - Hero Section & Branding
 *
 * Test Cases:
 * 1. Hero renders with MirDB title and ASCII logo
 * 2. Tagline contains required text
 * 3. CTA button exists with accessible name
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Hero from '$lib/components/sections/Hero.svelte';

describe('Hero Component', () => {
	// Test Case 1: Hero.svelte component renders with MirDB title, ASCII logo visible
	describe('Rendering and Branding', () => {
		it('should render the MirDB title', () => {
			render(Hero);
			const title = screen.getByRole('heading', { level: 1 });
			expect(title).toBeDefined();
			expect(title.textContent).toBe('MirDB');
		});

		it('should display the ASCII logo', () => {
			render(Hero);
			const logo = screen.getByLabelText('MirDB ASCII Logo');
			expect(logo).toBeDefined();
			expect(logo.textContent).toContain('███');
		});

		it('should have hero section with proper aria-label', () => {
			render(Hero);
			const heroSection = screen.getByRole('region', { name: 'Hero section' });
			expect(heroSection).toBeDefined();
		});
	});

	// Test Case 2: Tagline contains 'Persistent Key-Value Store' and 'Memcached'
	describe('Tagline', () => {
		it('should display tagline with Persistent Key-Value Store', () => {
			render(Hero);
			const tagline = screen.getByText(/Persistent Key-Value Store/i);
			expect(tagline).toBeDefined();
		});

		it('should display tagline with Memcached', () => {
			render(Hero);
			const tagline = screen.getByText(/Memcached/i);
			expect(tagline).toBeDefined();
		});

		it('should display the complete tagline', () => {
			render(Hero);
			const tagline = screen.getByText('A Persistent Key-Value Store with Memcached protocol');
			expect(tagline).toBeDefined();
		});
	});

	// Test Case 3: Button with role='button' or <button> element exists with accessible name
	describe('CTA Button', () => {
		it('should have a Get Started button', () => {
			render(Hero);
			const ctaButton = screen.getByRole('button', { name: 'Get Started' });
			expect(ctaButton).toBeDefined();
		});

		it('should have a View on GitHub link', () => {
			render(Hero);
			const githubLink = screen.getByRole('link', { name: 'View on GitHub' });
			expect(githubLink).toBeDefined();
			expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
		});

		it('should open GitHub link in new tab', () => {
			render(Hero);
			const githubLink = screen.getByRole('link', { name: 'View on GitHub' });
			expect(githubLink.getAttribute('target')).toBe('_blank');
			expect(githubLink.getAttribute('rel')).toContain('noopener');
		});
	});
});
