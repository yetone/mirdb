/**
 * Unit tests for ThemeToggle component
 * Owner: Scenario 5 - Theme Toggle
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/svelte';
import ThemeToggle from '$lib/components/ui/ThemeToggle.svelte';

describe('ThemeToggle Component', () => {
	// Mock localStorage
	let localStorageMock: Record<string, string> = {};

	// Mock matchMedia
	let darkModePreference = true;

	beforeEach(() => {
		// Reset localStorage mock
		localStorageMock = {};

		// Mock localStorage
		Object.defineProperty(window, 'localStorage', {
			value: {
				getItem: vi.fn((key: string) => localStorageMock[key] ?? null),
				setItem: vi.fn((key: string, value: string) => {
					localStorageMock[key] = value;
				}),
				removeItem: vi.fn((key: string) => {
					delete localStorageMock[key];
				}),
				clear: vi.fn(() => {
					localStorageMock = {};
				})
			},
			writable: true
		});

		// Mock matchMedia
		Object.defineProperty(window, 'matchMedia', {
			value: vi.fn((query: string) => ({
				matches: query === '(prefers-color-scheme: dark)' ? darkModePreference : false,
				media: query,
				onchange: null,
				addListener: vi.fn(),
				removeListener: vi.fn(),
				addEventListener: vi.fn(),
				removeEventListener: vi.fn(),
				dispatchEvent: vi.fn()
			})),
			writable: true
		});
	});

	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
	});

	describe('Rendering', () => {
		it('should render toggle button with accessible label (test case 1)', () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			expect(button).toBeInTheDocument();

			// Check for accessible label
			expect(button).toHaveAttribute('aria-label');
			const ariaLabel = button.getAttribute('aria-label');
			expect(ariaLabel).toMatch(/switch to (light|dark) theme/i);
		});

		it('should have aria-pressed attribute', () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			expect(button).toHaveAttribute('aria-pressed');
		});

		it('should have a title attribute for tooltip', () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			expect(button).toHaveAttribute('title');
		});

		it('should render an SVG icon', async () => {
			darkModePreference = true;
			render(ThemeToggle);

			// Wait for component to mount and render SVG
			await vi.waitFor(() => {
				const button = screen.getByRole('button');
				const svg = button.querySelector('svg');
				expect(svg).toBeInTheDocument();
			});
		});
	});

	describe('Interaction', () => {
		it('should toggle theme on click', async () => {
			darkModePreference = true;
			render(ThemeToggle);

			const button = screen.getByRole('button');

			// Initial state - check that it has an aria-label
			const initialLabel = button.getAttribute('aria-label');
			expect(initialLabel).toBeTruthy();

			// Click to toggle
			await fireEvent.click(button);

			// After click, localStorage should have been called
			await vi.waitFor(() => {
				expect(window.localStorage.setItem).toHaveBeenCalled();
			});
		});

		it('should toggle theme on Enter key press', async () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			await fireEvent.keyDown(button, { key: 'Enter' });

			// Theme should have toggled
			expect(window.localStorage.setItem).toHaveBeenCalled();
		});

		it('should toggle theme on Space key press', async () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			await fireEvent.keyDown(button, { key: ' ' });

			// Theme should have toggled
			expect(window.localStorage.setItem).toHaveBeenCalled();
		});
	});

	describe('Accessibility', () => {
		it('should be keyboard accessible', () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			expect(button).not.toHaveAttribute('tabindex', '-1');
		});

		it('should have visually hidden text for screen readers', () => {
			render(ThemeToggle);

			const hiddenText = document.querySelector('.visually-hidden');
			expect(hiddenText).toBeInTheDocument();
		});
	});
});
