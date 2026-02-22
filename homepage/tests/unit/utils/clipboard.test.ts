/**
 * Unit tests for clipboard utility functions.
 * Owner: Scenario 4 - Quick Start Section
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { copyToClipboard, isCopySupported } from '$lib/utils/clipboard';

describe('clipboard utility', () => {
	describe('isCopySupported', () => {
		it('returns true when clipboard API is available', () => {
			// Mock navigator.clipboard
			const originalNavigator = global.navigator;
			Object.defineProperty(global, 'navigator', {
				value: {
					clipboard: {
						writeText: vi.fn()
					}
				},
				configurable: true
			});

			expect(isCopySupported()).toBe(true);

			// Restore
			Object.defineProperty(global, 'navigator', {
				value: originalNavigator,
				configurable: true
			});
		});

		it('returns false when clipboard API is not available', () => {
			const originalNavigator = global.navigator;
			Object.defineProperty(global, 'navigator', {
				value: {},
				configurable: true
			});

			expect(isCopySupported()).toBe(false);

			// Restore
			Object.defineProperty(global, 'navigator', {
				value: originalNavigator,
				configurable: true
			});
		});

		it('returns false when navigator is undefined', () => {
			const originalNavigator = global.navigator;
			// @ts-expect-error - Testing edge case
			delete global.navigator;

			expect(isCopySupported()).toBe(false);

			// Restore
			Object.defineProperty(global, 'navigator', {
				value: originalNavigator,
				configurable: true
			});
		});
	});

	describe('copyToClipboard', () => {
		let originalNavigator: Navigator;
		let mockWriteText: ReturnType<typeof vi.fn>;

		beforeEach(() => {
			originalNavigator = global.navigator;
			mockWriteText = vi.fn().mockResolvedValue(undefined);
			Object.defineProperty(global, 'navigator', {
				value: {
					clipboard: {
						writeText: mockWriteText
					}
				},
				configurable: true
			});
		});

		afterEach(() => {
			Object.defineProperty(global, 'navigator', {
				value: originalNavigator,
				configurable: true
			});
		});

		it('returns Promise<boolean> indicating success', async () => {
			const result = await copyToClipboard('test text');
			expect(typeof result).toBe('boolean');
			expect(result).toBe(true);
		});

		it('calls clipboard.writeText with the provided text', async () => {
			const text = 'cargo install mirdb';
			await copyToClipboard(text);
			expect(mockWriteText).toHaveBeenCalledWith(text);
		});

		it('returns true on successful copy', async () => {
			const result = await copyToClipboard('test text');
			expect(result).toBe(true);
		});

		it('returns false when clipboard API throws an error', async () => {
			mockWriteText.mockRejectedValue(new Error('Permission denied'));

			// Mock document.execCommand for fallback
			const mockExecCommand = vi.fn().mockReturnValue(false);
			Object.defineProperty(document, 'execCommand', {
				value: mockExecCommand,
				configurable: true
			});

			const result = await copyToClipboard('test text');
			expect(result).toBe(false);
		});

		it('uses fallback when clipboard API is not available', async () => {
			Object.defineProperty(global, 'navigator', {
				value: {},
				configurable: true
			});

			// Mock document.execCommand
			const mockExecCommand = vi.fn().mockReturnValue(true);
			Object.defineProperty(document, 'execCommand', {
				value: mockExecCommand,
				configurable: true
			});

			const result = await copyToClipboard('test text');
			expect(mockExecCommand).toHaveBeenCalledWith('copy');
			expect(result).toBe(true);
		});
	});
});
