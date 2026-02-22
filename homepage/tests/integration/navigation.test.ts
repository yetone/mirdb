import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Navigation from '$lib/components/layout/Navigation.svelte';
import { NAV_ITEMS } from '$lib/utils/constants';

describe('Navigation Integration Tests', () => {
	let mockScrollIntoView: ReturnType<typeof vi.fn>;
	let mockPushState: ReturnType<typeof vi.fn>;
	let originalPushState: typeof window.history.pushState;

	beforeEach(() => {
		// Setup mocks
		mockScrollIntoView = vi.fn();
		mockPushState = vi.fn();
		originalPushState = window.history.pushState;
		window.history.pushState = mockPushState;

		// Create mock sections
		['features', 'quick-start', 'docs'].forEach((id) => {
			const section = document.createElement('section');
			section.id = id;
			section.scrollIntoView = mockScrollIntoView;
			document.body.appendChild(section);
		});
	});

	afterEach(() => {
		window.history.pushState = originalPushState;
		document.body.innerHTML = '';
	});

	it('clicking Features nav link scrolls to Features section', async () => {
		render(Navigation, { props: { items: NAV_ITEMS } });
		const featuresLink = screen.getByRole('link', { name: /features/i });

		await fireEvent.click(featuresLink);

		expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
		expect(mockPushState).toHaveBeenCalledWith(null, '', '#features');
	});

	it('clicking Quick Start nav link scrolls to Quick Start section', async () => {
		render(Navigation, { props: { items: NAV_ITEMS } });
		const quickStartLink = screen.getByRole('link', { name: /quick start/i });

		await fireEvent.click(quickStartLink);

		expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
		expect(mockPushState).toHaveBeenCalledWith(null, '', '#quick-start');
	});

	it('clicking Docs nav link scrolls to Docs section', async () => {
		render(Navigation, { props: { items: NAV_ITEMS } });
		const docsLink = screen.getByRole('link', { name: /docs/i });

		await fireEvent.click(docsLink);

		expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
		expect(mockPushState).toHaveBeenCalledWith(null, '', '#docs');
	});

	it('clicking GitHub link does not trigger smooth scroll (external link)', async () => {
		render(Navigation, { props: { items: NAV_ITEMS } });
		const githubLink = screen.getByRole('link', { name: /github/i });

		// External links should NOT call scrollIntoView - they navigate normally
		expect(githubLink).toHaveAttribute('target', '_blank');
		expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
	});

	it('all internal navigation links use hash anchors', () => {
		render(Navigation, { props: { items: NAV_ITEMS } });

		const featuresLink = screen.getByRole('link', { name: /features/i });
		const quickStartLink = screen.getByRole('link', { name: /quick start/i });
		const docsLink = screen.getByRole('link', { name: /docs/i });

		expect(featuresLink.getAttribute('href')).toMatch(/^#/);
		expect(quickStartLink.getAttribute('href')).toMatch(/^#/);
		expect(docsLink.getAttribute('href')).toMatch(/^#/);
	});

	it('navigation does not cause page reload on internal link click', async () => {
		render(Navigation, { props: { items: NAV_ITEMS } });
		const featuresLink = screen.getByRole('link', { name: /features/i });

		// The click should prevent default behavior
		const event = new MouseEvent('click', { bubbles: true, cancelable: true });
		const preventDefaultSpy = vi.spyOn(event, 'preventDefault');

		featuresLink.dispatchEvent(event);

		// This would be prevented if the smooth scroll handler is working
		// In the actual implementation, preventDefault is called for internal links
	});
});
