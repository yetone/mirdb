/**
 * Unit tests for QuickStart section component.
 * Owner: Scenario 4 - Quick Start Section
 */
import { render, screen } from '@testing-library/svelte';
import { describe, it, expect, vi } from 'vitest';
import QuickStart from '$lib/components/sections/QuickStart.svelte';

// Mock the clipboard utility
vi.mock('$lib/utils/clipboard', () => ({
	copyToClipboard: vi.fn().mockResolvedValue(true)
}));

describe('QuickStart', () => {
	it('contains Quick Start heading', () => {
		render(QuickStart);

		expect(screen.getByRole('heading', { name: 'Quick Start' })).toBeInTheDocument();
	});

	it('contains section with correct data-testid', () => {
		render(QuickStart);

		expect(screen.getByTestId('quickstart-section')).toBeInTheDocument();
	});

	it('contains installation instructions with cargo command', () => {
		render(QuickStart);

		expect(screen.getByText('cargo install mirdb')).toBeInTheDocument();
	});

	it('contains git clone installation option', () => {
		render(QuickStart);

		expect(
			screen.getByText(
				'git clone https://github.com/yetone/mirdb.git && cd mirdb && cargo build --release'
			)
		).toBeInTheDocument();
	});

	it('contains usage examples for set operation', () => {
		render(QuickStart);

		// Check for set operation
		expect(screen.getByText(/set mykey/)).toBeInTheDocument();
	});

	it('contains usage examples for get operation', () => {
		render(QuickStart);

		// Check for get operation
		expect(screen.getByText(/get mykey/)).toBeInTheDocument();
	});

	it('contains usage examples for delete operation', () => {
		render(QuickStart);

		// Check for delete operation
		expect(screen.getByText(/delete mykey/)).toBeInTheDocument();
	});

	it('has section with id="quickstart" for navigation', () => {
		render(QuickStart);

		const section = screen.getByTestId('quickstart-section');
		expect(section).toHaveAttribute('id', 'quickstart');
	});

	it('contains multiple code blocks', () => {
		render(QuickStart);

		const codeBlocks = screen.getAllByTestId('code-block');
		expect(codeBlocks.length).toBeGreaterThanOrEqual(3);
	});

	it('contains copy buttons for code blocks', () => {
		render(QuickStart);

		const copyButtons = screen.getAllByTestId('copy-button');
		expect(copyButtons.length).toBeGreaterThanOrEqual(3);
	});
});
