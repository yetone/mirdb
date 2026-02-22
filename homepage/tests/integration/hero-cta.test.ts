/**
 * Hero CTA Integration Tests
 * Owner: Scenario 1 - Hero Section & Branding
 *
 * Test Case 4: Click CTA button -> Page scrolls smoothly to Quick Start section
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Hero from '$lib/components/sections/Hero.svelte';

describe('Hero CTA Integration', () => {
	let scrollIntoViewMock: ReturnType<typeof vi.fn>;
	let getElementByIdSpy: ReturnType<typeof vi.spyOn>;

	beforeEach(() => {
		// Mock scrollIntoView
		scrollIntoViewMock = vi.fn();

		// Create mock quickstart element
		const mockQuickStartSection = document.createElement('section');
		mockQuickStartSection.id = 'quickstart';
		mockQuickStartSection.scrollIntoView = scrollIntoViewMock;

		// Spy on getElementById
		getElementByIdSpy = vi.spyOn(document, 'getElementById');
		getElementByIdSpy.mockImplementation((id: string) => {
			if (id === 'quickstart') {
				return mockQuickStartSection;
			}
			return null;
		});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	// Test Case 4: Click CTA button -> Page scrolls smoothly to Quick Start section
	it('should scroll to Quick Start section when CTA button is clicked', async () => {
		render(Hero);

		const ctaButton = screen.getByRole('button', { name: 'Get Started' });
		expect(ctaButton).toBeDefined();

		// Click the CTA button
		await fireEvent.click(ctaButton);

		// Verify scrollIntoView was called with smooth behavior
		expect(scrollIntoViewMock).toHaveBeenCalledTimes(1);
		expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });
	});

	it('should look for quickstart element when CTA is clicked', async () => {
		render(Hero);

		const ctaButton = screen.getByRole('button', { name: 'Get Started' });
		await fireEvent.click(ctaButton);

		expect(getElementByIdSpy).toHaveBeenCalledWith('quickstart');
	});

	it('should not throw error if quickstart section does not exist', async () => {
		// Mock getElementById to return null
		getElementByIdSpy.mockReturnValue(null);

		render(Hero);

		const ctaButton = screen.getByRole('button', { name: 'Get Started' });

		// Should not throw
		await expect(fireEvent.click(ctaButton)).resolves.toBeDefined();
	});
});
