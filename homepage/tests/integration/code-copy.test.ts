/**
 * Integration tests for code copy functionality.
 * Owner: Scenario 4 - Quick Start Section
 *
 * Tests the complete copy functionality across components.
 */
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import CodeBlock from '$lib/components/ui/CodeBlock.svelte';
import QuickStart from '$lib/components/sections/QuickStart.svelte';

describe('Code Copy Integration', () => {
	let mockWriteText: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		// Mock navigator.clipboard
		mockWriteText = vi.fn().mockResolvedValue(undefined);
		Object.defineProperty(navigator, 'clipboard', {
			value: {
				writeText: mockWriteText
			},
			configurable: true
		});
	});

	afterEach(() => {
		vi.clearAllMocks();
	});

	describe('CodeBlock with CopyButton', () => {
		it('clicking copy button copies code content to clipboard', async () => {
			const code = 'cargo install mirdb';
			render(CodeBlock, { props: { code, language: 'bash' } });

			const copyButton = screen.getByTestId('copy-button');
			await fireEvent.click(copyButton);

			expect(mockWriteText).toHaveBeenCalledWith(code);
		});

		it('copy button shows success state after copying', async () => {
			render(CodeBlock, { props: { code: 'test code', language: 'bash' } });

			const copyButton = screen.getByTestId('copy-button');
			await fireEvent.click(copyButton);

			await waitFor(() => {
				expect(copyButton).toHaveClass('copied');
				expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
			});
		});

		it('preserves exact code content when copying', async () => {
			const complexCode = `echo -e "set mykey 0 0 5\\r\\nvalue\\r\\n" | nc localhost 12333`;
			render(CodeBlock, { props: { code: complexCode, language: 'bash' } });

			const copyButton = screen.getByTestId('copy-button');
			await fireEvent.click(copyButton);

			expect(mockWriteText).toHaveBeenCalledWith(complexCode);
		});
	});

	describe('QuickStart section copy functionality', () => {
		it('all code blocks have functional copy buttons', async () => {
			render(QuickStart);

			const copyButtons = screen.getAllByTestId('copy-button');
			expect(copyButtons.length).toBeGreaterThan(0);

			// Click first copy button
			await fireEvent.click(copyButtons[0]);

			// Should have been called with some code content
			expect(mockWriteText).toHaveBeenCalled();
		});

		it('clicking copy button on installation code copies cargo command', async () => {
			render(QuickStart);

			// Get all code blocks
			const codeBlocks = screen.getAllByTestId('code-block');

			// Find the first copy button (associated with cargo install)
			const firstCopyButton = codeBlocks[0].querySelector('[data-testid="copy-button"]');
			expect(firstCopyButton).not.toBeNull();

			await fireEvent.click(firstCopyButton!);

			expect(mockWriteText).toHaveBeenCalledWith('cargo install mirdb');
		});

		it('multiple copy operations work independently', async () => {
			render(QuickStart);

			const copyButtons = screen.getAllByTestId('copy-button');

			// Click first button
			await fireEvent.click(copyButtons[0]);

			await waitFor(() => {
				expect(copyButtons[0]).toHaveClass('copied');
			});

			// Click second button
			await fireEvent.click(copyButtons[1]);

			await waitFor(() => {
				expect(copyButtons[1]).toHaveClass('copied');
			});

			// Both should have been called
			expect(mockWriteText).toHaveBeenCalledTimes(2);
		});
	});

	describe('Clipboard API behavior', () => {
		it('handles clipboard API failure gracefully', async () => {
			// Mock clipboard to fail
			mockWriteText.mockRejectedValue(new Error('Permission denied'));

			// Mock execCommand as fallback by defining it on document
			const mockExecCommand = vi.fn().mockReturnValue(true);
			Object.defineProperty(document, 'execCommand', {
				value: mockExecCommand,
				configurable: true,
				writable: true
			});

			render(CodeBlock, { props: { code: 'test', language: 'bash' } });

			const copyButton = screen.getByTestId('copy-button');
			await fireEvent.click(copyButton);

			// Should attempt clipboard API first
			expect(mockWriteText).toHaveBeenCalled();

			// Fallback should be attempted
			expect(mockExecCommand).toHaveBeenCalledWith('copy');
		});

		it('code block shows success state when fallback copy works', async () => {
			// Mock clipboard to fail
			mockWriteText.mockRejectedValue(new Error('Permission denied'));

			// Mock execCommand as successful fallback by defining it on document
			const mockExecCommand = vi.fn().mockReturnValue(true);
			Object.defineProperty(document, 'execCommand', {
				value: mockExecCommand,
				configurable: true,
				writable: true
			});

			render(CodeBlock, { props: { code: 'test', language: 'bash' } });

			const copyButton = screen.getByTestId('copy-button');
			await fireEvent.click(copyButton);

			await waitFor(() => {
				expect(copyButton).toHaveClass('copied');
			});
		});
	});
});
