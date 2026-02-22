/**
 * Unit tests for CopyButton component.
 * Owner: Scenario 4 - Quick Start Section
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import CopyButton from '$lib/components/ui/CopyButton.svelte';

// Mock the clipboard utility
vi.mock('$lib/utils/clipboard', () => ({
	copyToClipboard: vi.fn()
}));

import { copyToClipboard } from '$lib/utils/clipboard';

describe('CopyButton', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('renders button with copy icon/text', () => {
		render(CopyButton, { props: { text: 'test code' } });

		const button = screen.getByTestId('copy-button');
		expect(button).toBeInTheDocument();
		expect(button).toHaveAttribute('aria-label', 'Copy to clipboard');
	});

	it('renders with correct data-testid attribute', () => {
		render(CopyButton, { props: { text: 'test code' } });

		expect(screen.getByTestId('copy-button')).toBeInTheDocument();
	});

	it('has correct aria-label for accessibility', () => {
		render(CopyButton, { props: { text: 'test code' } });

		const button = screen.getByTestId('copy-button');
		expect(button).toHaveAttribute('aria-label', 'Copy to clipboard');
	});

	it('calls copyToClipboard with the provided text on click', async () => {
		vi.mocked(copyToClipboard).mockResolvedValue(true);

		const textToCopy = 'cargo install mirdb';
		render(CopyButton, { props: { text: textToCopy } });

		const button = screen.getByTestId('copy-button');
		await fireEvent.click(button);

		expect(copyToClipboard).toHaveBeenCalledWith(textToCopy);
	});

	it('shows success state after successful copy', async () => {
		vi.mocked(copyToClipboard).mockResolvedValue(true);

		render(CopyButton, { props: { text: 'test code' } });

		const button = screen.getByTestId('copy-button');
		await fireEvent.click(button);

		await waitFor(() => {
			expect(button).toHaveAttribute('aria-label', 'Copied!');
			expect(button).toHaveClass('copied');
		});
	});

	it('dispatches copied event on successful copy', async () => {
		vi.mocked(copyToClipboard).mockResolvedValue(true);

		const mockHandler = vi.fn();
		const { component } = render(CopyButton, { props: { text: 'test code' } });
		component.$on('copied', mockHandler);

		const button = screen.getByTestId('copy-button');
		await fireEvent.click(button);

		await waitFor(() => {
			expect(mockHandler).toHaveBeenCalled();
		});
	});

	it('does not show success state when copy fails', async () => {
		vi.mocked(copyToClipboard).mockResolvedValue(false);

		render(CopyButton, { props: { text: 'test code' } });

		const button = screen.getByTestId('copy-button');
		await fireEvent.click(button);

		// Button should not have copied class
		expect(button).not.toHaveClass('copied');
		expect(button).toHaveAttribute('aria-label', 'Copy to clipboard');
	});

	it('resets success state after 2 seconds', async () => {
		vi.useFakeTimers();
		vi.mocked(copyToClipboard).mockResolvedValue(true);

		render(CopyButton, { props: { text: 'test code' } });

		const button = screen.getByTestId('copy-button');
		await fireEvent.click(button);

		await waitFor(() => {
			expect(button).toHaveClass('copied');
		});

		// Fast-forward time
		vi.advanceTimersByTime(2000);

		await waitFor(() => {
			expect(button).not.toHaveClass('copied');
		});

		vi.useRealTimers();
	});
});
