<!--
  Copy Button Component
  Owner: Scenario 4 - Quick Start Section

  Props:
  - text: string (content to copy)

  Events:
  - copied: CustomEvent (on successful copy)

  Shows visual feedback on copy success
-->
<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import { copyToClipboard } from '$lib/utils/clipboard';

	export let text: string;

	const dispatch = createEventDispatcher<{ copied: void }>();

	let copied = false;
	let copyTimeout: ReturnType<typeof setTimeout>;

	async function handleCopy() {
		const success = await copyToClipboard(text);
		if (success) {
			copied = true;
			dispatch('copied');

			// Reset copied state after 2 seconds
			clearTimeout(copyTimeout);
			copyTimeout = setTimeout(() => {
				copied = false;
			}, 2000);
		}
	}
</script>

<button
	type="button"
	class="copy-button"
	class:copied
	on:click={handleCopy}
	aria-label={copied ? 'Copied!' : 'Copy to clipboard'}
	data-testid="copy-button"
>
	{#if copied}
		<svg
			xmlns="http://www.w3.org/2000/svg"
			width="16"
			height="16"
			viewBox="0 0 24 24"
			fill="none"
			stroke="currentColor"
			stroke-width="2"
			stroke-linecap="round"
			stroke-linejoin="round"
			aria-hidden="true"
		>
			<polyline points="20 6 9 17 4 12"></polyline>
		</svg>
		<span class="copy-text">Copied!</span>
	{:else}
		<svg
			xmlns="http://www.w3.org/2000/svg"
			width="16"
			height="16"
			viewBox="0 0 24 24"
			fill="none"
			stroke="currentColor"
			stroke-width="2"
			stroke-linecap="round"
			stroke-linejoin="round"
			aria-hidden="true"
		>
			<rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
			<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
		</svg>
		<span class="copy-text">Copy</span>
	{/if}
</button>

<style>
	.copy-button {
		display: inline-flex;
		align-items: center;
		gap: var(--spacing-2);
		padding: var(--spacing-2) var(--spacing-3);
		background-color: transparent;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-md);
		color: var(--color-text-muted);
		font-size: var(--font-size-sm);
		font-family: var(--font-family);
		cursor: pointer;
		transition: all var(--transition-fast);
	}

	.copy-button:hover {
		background-color: var(--color-surface);
		color: var(--color-text);
		border-color: var(--color-text-muted);
	}

	.copy-button:focus-visible {
		outline: 2px solid var(--color-primary);
		outline-offset: 2px;
	}

	.copy-button.copied {
		color: var(--color-success);
		border-color: var(--color-success);
	}

	.copy-text {
		display: none;
	}

	@media (min-width: 768px) {
		.copy-text {
			display: inline;
		}
	}
</style>
