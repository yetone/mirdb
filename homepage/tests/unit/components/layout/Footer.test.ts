import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import Footer from '$lib/components/layout/Footer.svelte';

describe('Footer Component', () => {
	it('renders Footer component', () => {
		render(Footer);
		const footer = document.querySelector('footer');
		expect(footer).toBeInTheDocument();
	});

	it('contains copyright text', () => {
		render(Footer);
		const currentYear = new Date().getFullYear();
		expect(screen.getByText(new RegExp(`© ${currentYear} MirDB`, 'i'))).toBeInTheDocument();
	});

	it('contains MirDB branding', () => {
		render(Footer);
		const brandText = screen.getByText('MirDB');
		expect(brandText).toBeInTheDocument();
	});

	it('contains tagline text', () => {
		render(Footer);
		expect(screen.getByText(/persistent key-value store/i)).toBeInTheDocument();
	});

	it('contains GitHub link', () => {
		render(Footer);
		const githubLink = screen.getByRole('link', { name: /github/i });
		expect(githubLink).toBeInTheDocument();
	});

	it('GitHub link has correct href and opens in new tab', () => {
		render(Footer);
		const githubLink = screen.getByRole('link', { name: /github/i });
		expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
		expect(githubLink).toHaveAttribute('target', '_blank');
		expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
	});

	it('contains GitHub icon (SVG)', () => {
		render(Footer);
		const svg = document.querySelector('.github-icon');
		expect(svg).toBeInTheDocument();
	});
});
