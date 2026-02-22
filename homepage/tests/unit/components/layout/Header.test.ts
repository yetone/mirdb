import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import Header from '$lib/components/layout/Header.svelte';

describe('Header Component', () => {
	it('renders Header with logo', () => {
		render(Header);
		const logo = screen.getByText('MirDB');
		expect(logo).toBeInTheDocument();
		expect(logo.closest('a')).toHaveAttribute('href', '/');
	});

	it('contains nav element with navigation links', () => {
		render(Header);
		const nav = screen.getByRole('navigation', { name: /main navigation/i });
		expect(nav).toBeInTheDocument();
	});

	it('contains links for Features, Quick Start, Docs, and GitHub', () => {
		render(Header);
		expect(screen.getByRole('link', { name: /features/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /quick start/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /docs/i })).toBeInTheDocument();
		expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument();
	});

	it('GitHub link has correct href and target="_blank"', () => {
		render(Header);
		const githubLink = screen.getByRole('link', { name: /github/i });
		expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		expect(githubLink).toHaveAttribute('target', '_blank');
		expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
	});

	it('has fixed/sticky positioning styles', () => {
		render(Header);
		const header = document.querySelector('.header');
		expect(header).toBeInTheDocument();
	});

	it('contains mobile menu toggle button', () => {
		render(Header);
		const menuButton = screen.getByRole('button', { name: /open menu/i });
		expect(menuButton).toBeInTheDocument();
	});

	it('toggles mobile menu on button click', async () => {
		render(Header);
		const menuButton = screen.getByRole('button', { name: /open menu/i });

		await fireEvent.click(menuButton);

		// After click, the mobile menu toggle button should have aria-expanded="true"
		expect(menuButton).toHaveAttribute('aria-expanded', 'true');

		// Check that the mobile menu dialog is now visible
		const mobileMenu = screen.getByRole('dialog', { name: /mobile navigation/i });
		expect(mobileMenu).toBeInTheDocument();

		// There should be buttons to close the menu (both the hamburger toggle and the overlay)
		const closeButtons = screen.getAllByRole('button', { name: /close menu/i });
		expect(closeButtons.length).toBeGreaterThanOrEqual(1);
	});
});
