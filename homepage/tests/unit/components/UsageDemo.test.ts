/**
 * Usage Demo Section Component Tests
 * Owner: Scenario 10 - Usage Demo Section
 *
 * Test Cases:
 * 1. Component renders demo container with image element
 * 2. Source points to usage.gif demo asset
 * 3. Demo element has loading='lazy' attribute
 * 4. Demo has alt text or aria-label describing the demonstration
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import UsageDemo from '$lib/components/sections/UsageDemo.svelte';

describe('UsageDemo Component', () => {
	// Test Case 1: Component renders demo container with image/video element
	describe('Rendering', () => {
		it('should render the demo section', () => {
			render(UsageDemo);
			const section = screen.getByRole('region', { name: /usage demo/i });
			expect(section).toBeDefined();
		});

		it('should render a heading', () => {
			render(UsageDemo);
			const heading = screen.getByRole('heading', { level: 2, name: /usage demo/i });
			expect(heading).toBeDefined();
		});

		it('should render demo container with image element', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			expect(image).toBeDefined();
		});

		it('should render the demo description text', () => {
			render(UsageDemo);
			const description = screen.getByText(/See MirDB in action/i);
			expect(description).toBeDefined();
		});
	});

	// Test Case 2: Source points to usage.gif or equivalent demo asset
	describe('Demo Source', () => {
		it('should have image source pointing to usage.gif', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			expect(image.getAttribute('src')).toBe('/assets/usage.gif');
		});

		it('should render animated GIF by default', () => {
			render(UsageDemo, { props: { animated: true } });
			const image = screen.getByRole('img');
			expect(image).toBeDefined();
			expect(image.getAttribute('src')).toContain('usage.gif');
		});

		it('should support static mode via animated prop', () => {
			render(UsageDemo, { props: { animated: false } });
			const image = screen.getByRole('img');
			expect(image.classList.contains('demo-static')).toBe(true);
		});
	});

	// Test Case 3: Demo element has loading='lazy' attribute
	describe('Lazy Loading', () => {
		it('should have loading="lazy" attribute on image', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			expect(image.getAttribute('loading')).toBe('lazy');
		});

		it('should have lazy loading for performance optimization', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			// Verify the image has native lazy loading
			expect(image.getAttribute('loading')).toBe('lazy');
		});
	});

	// Test Case 4: Demo has alt text or aria-label describing the demonstration
	describe('Accessibility', () => {
		it('should have descriptive alt text on image', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			const altText = image.getAttribute('alt');
			expect(altText).toBeDefined();
			expect(altText).not.toBe('');
			expect(altText?.length).toBeGreaterThan(20);
		});

		it('should have alt text mentioning MirDB', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			const altText = image.getAttribute('alt');
			expect(altText).toContain('MirDB');
		});

		it('should have alt text mentioning Memcached protocol', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			const altText = image.getAttribute('alt');
			expect(altText).toContain('Memcached');
		});

		it('should have aria-describedby for extended description', () => {
			render(UsageDemo);
			const image = screen.getByRole('img');
			expect(image.getAttribute('aria-describedby')).toBe('demo-description');
		});

		it('should have a hidden extended description', () => {
			render(UsageDemo);
			const description = screen.getByText(/This demonstration shows a terminal session/i);
			expect(description).toBeDefined();
			expect(description.classList.contains('sr-only')).toBe(true);
		});

		it('should have section with proper aria-labelledby', () => {
			render(UsageDemo);
			const section = screen.getByRole('region');
			expect(section.getAttribute('aria-labelledby')).toBe('usage-demo-heading');
		});
	});

	// Additional rendering tests
	describe('Section Structure', () => {
		it('should have the correct section id', () => {
			render(UsageDemo);
			const section = screen.getByRole('region');
			expect(section.getAttribute('id')).toBe('usage-demo');
		});

		it('should have demo container wrapper', () => {
			render(UsageDemo);
			const container = document.querySelector('.demo-container');
			expect(container).toBeDefined();
		});
	});
});
