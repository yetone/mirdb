/**
 * Clipboard utility functions.
 * Owner: Scenario 4 - Quick Start Section
 *
 * Provides functions for copying text to clipboard with proper
 * browser API support detection and fallback handling.
 */

/**
 * Checks if the clipboard API is supported in the current browser.
 * @returns true if clipboard API is available
 */
export function isCopySupported(): boolean {
	if (typeof navigator === 'undefined') {
		return false;
	}
	return !!(navigator.clipboard && navigator.clipboard.writeText);
}

/**
 * Copies text to the clipboard using the Clipboard API.
 * @param text - The text to copy to clipboard
 * @returns Promise resolving to true on success, false on failure
 */
export async function copyToClipboard(text: string): Promise<boolean> {
	if (!isCopySupported()) {
		return fallbackCopy(text);
	}

	try {
		await navigator.clipboard.writeText(text);
		return true;
	} catch (error) {
		// Fallback for cases where clipboard API fails (e.g., permissions denied)
		return fallbackCopy(text);
	}
}

/**
 * Fallback copy method using execCommand for older browsers.
 * @param text - The text to copy
 * @returns true on success, false on failure
 */
function fallbackCopy(text: string): boolean {
	if (typeof document === 'undefined') {
		return false;
	}

	const textArea = document.createElement('textarea');
	textArea.value = text;

	// Avoid scrolling to bottom
	textArea.style.top = '0';
	textArea.style.left = '0';
	textArea.style.position = 'fixed';
	textArea.style.opacity = '0';
	textArea.style.pointerEvents = 'none';

	document.body.appendChild(textArea);
	textArea.focus();
	textArea.select();

	let success = false;
	try {
		success = document.execCommand('copy');
	} catch (error) {
		success = false;
	}

	document.body.removeChild(textArea);
	return success;
}
