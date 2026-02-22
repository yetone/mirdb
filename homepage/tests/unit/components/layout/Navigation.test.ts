import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Navigation from '$lib/components/layout/Navigation.svelte';
import type { NavItem } from '$lib/types';

const mockNavItems: NavItem[] = [
	{ label: 'Features', href: '#features' },
	{ label: 'Quick Start', href: '#quick-start' },
	{ label: 'Docs', href: '#docs' },
	{ label: 'GitHub', href: 'https://github.com/yetone/mirdb', external: true }
];

describe('Navigation Component', () => {
	it('renders navigation with correct aria label', () => {
		render(Navigation, { props: { items: mockNavItems } });
		const nav = screen.getByRole('navigation', { name: /main navigation/i });
		expect(nav).toBeInTheDocument();
	});

	it('renders all navigation links', () => {
		render(Navigation, { props: { items: mockNavItems } });
		expect(screen.getByRole('link', { name: /features/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /quick start/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /docs/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument();
	});

	it('internal links have correct href attributes', () => {
		render(Navigation, { props: { items: mockNavItems } });
		expect(screen.getByRole('link', { name: /features/i })).toHaveAttribute('href', '#features');
		expect(screen.getByRole('link', { name: /quick start/i })).toHaveAttribute('href', '#quick-start');
		expect(screen.getByRole('link', { name: /docs/i })).toHaveAttribute('href', '#docs');
	});

	it('external link has target="_blank" and rel attributes', () => {
		render(Navigation, { props: { items: mockNavItems } });
		const githubLink = screen.getByRole('link', { name: /github/i });
		expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		expect(githubLink).toHaveAttribute('target', '_blank');
		expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
	});

	it('applies mobile class when mobile prop is true', () => {
		render(Navigation, { props: { items: mockNavItems, mobile: true } });
		const nav = screen.getByRole('navigation');
		expect(nav).toHaveClass('mobile');
	});

	it('does not apply mobile class by default', () => {
		render(Navigation, { props: { items: mockNavItems } });
		const nav = screen.getByRole('navigation');
		expect(nav).not.toHaveClass('mobile');
	});

	it('handles internal link click with smooth scroll behavior', async () => {
		// Mock scrollIntoView
		const mockScrollIntoView = vi.fn();
		Element.prototype.scrollIntoView = mockScrollIntoView;

		// Mock querySelector to return a target element
		const originalQuerySelector = document.querySelector.bind(document);
		document.querySelector = vi.fn((selector: string) => {
			if (selector === '#features') {
				const div = document.createElement('div');
				div.scrollIntoView = mockScrollIntoView;
				return div;
			}
			return originalQuerySelector(selector);
		});

		// Mock history.pushState
		const mockPushState = vi.fn();
		const originalPushState = window.history.pushState;
		window.history.pushState = mockPushState;

		render(Navigation, { props: { items: mockNavItems } });
		const featuresLink = screen.getByRole('link', { name: /features/i });

		await fireEvent.click(featuresLink);

		expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
		expect(mockPushState).toHaveBeenCalledWith(null, '', '#features');

		// Cleanup
		document.querySelector = originalQuerySelector;
		window.history.pushState = originalPushState;
	});
});
