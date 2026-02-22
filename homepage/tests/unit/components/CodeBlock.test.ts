/**
 * Unit tests for CodeBlock component.
 * Owner: Scenario 4 - Quick Start Section
 */
import { render, screen } from '@testing-library/svelte';
import { describe, it, expect, vi } from 'vitest';
import CodeBlock from '$lib/components/ui/CodeBlock.svelte';

// Mock the clipboard utility
vi.mock('$lib/utils/clipboard', () => ({
	copyToClipboard: vi.fn().mockResolvedValue(true)
}));

describe('CodeBlock', () => {
	it('renders code content correctly', () => {
		const code = 'cargo install mirdb';
		render(CodeBlock, { props: { code, language: 'bash' } });

		expect(screen.getByText(code)).toBeInTheDocument();
	});

	it('displays code with syntax highlighting classes', () => {
		const code = 'git clone https://github.com/yetone/mirdb.git';
		render(CodeBlock, { props: { code, language: 'bash' } });

		const codeElement = screen.getByText(code);
		expect(codeElement).toHaveClass('language-bash');
	});

	it('renders with data-testid attribute', () => {
		render(CodeBlock, { props: { code: 'test', language: 'bash' } });

		expect(screen.getByTestId('code-block')).toBeInTheDocument();
	});

	it('shows language label in the header', () => {
		render(CodeBlock, { props: { code: 'test code', language: 'bash' } });

		expect(screen.getByText('bash')).toBeInTheDocument();
	});

	it('renders copy button by default', () => {
		render(CodeBlock, { props: { code: 'test code', language: 'bash' } });

		expect(screen.getByTestId('copy-button')).toBeInTheDocument();
	});

	it('hides copy button when showCopy is false', () => {
		render(CodeBlock, { props: { code: 'test code', language: 'bash', showCopy: false } });

		expect(screen.queryByTestId('copy-button')).not.toBeInTheDocument();
	});

	it('renders with data-language attribute', () => {
		render(CodeBlock, { props: { code: 'test', language: 'javascript' } });

		const codeBlock = screen.getByTestId('code-block');
		expect(codeBlock).toHaveAttribute('data-language', 'javascript');
	});

	it('renders pre and code elements for proper formatting', () => {
		const code = 'echo "hello world"';
		render(CodeBlock, { props: { code, language: 'bash' } });

		const codeElement = screen.getByText(code);
		expect(codeElement.tagName).toBe('CODE');
		expect(codeElement.parentElement?.tagName).toBe('PRE');
	});

	it('preserves whitespace in code content', () => {
		const code = 'line1\n  indented\n    more indented';
		render(CodeBlock, { props: { code, language: 'bash' } });

		// Use a function matcher since getByText normalizes whitespace
		const codeElement = screen.getByText((content, element) => {
			return element?.tagName === 'CODE' && element.textContent === code;
		});
		expect(codeElement).toBeInTheDocument();
	});

	it('defaults language to bash when not specified', () => {
		render(CodeBlock, { props: { code: 'test' } });

		expect(screen.getByText('bash')).toBeInTheDocument();
	});
});
