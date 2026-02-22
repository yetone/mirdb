<!--
  Theme Toggle Component
  Owner: Scenario 5 - Theme Toggle

  Switches between light and dark themes.
  Persists preference to localStorage.
  Respects system preference on first visit.

  Events:
  - change: CustomEvent<Theme>

  NFR-6 traceability
-->
<script lang="ts">
	import { theme, toggleTheme, initTheme } from '$lib/stores/theme';
	import { onMount, createEventDispatcher } from 'svelte';
	import type { Theme } from '$lib/types';

	const dispatch = createEventDispatcher<{ change: Theme }>();

	let currentTheme = $state<Theme>('dark');
	let mounted = $state(false);

	onMount(() => {
		initTheme();
		mounted = true;

		const unsubscribe = theme.subscribe((value) => {
			currentTheme = value;
			dispatch('change', value);
		});

		return unsubscribe;
	});

	function handleToggle() {
		toggleTheme();
	}

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Enter' || event.key === ' ') {
			event.preventDefault();
			handleToggle();
		}
	}
</script>

<button
	type="button"
	class="theme-toggle"
	onclick={handleToggle}
	onkeydown={handleKeydown}
	aria-label={currentTheme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
	aria-pressed={currentTheme === 'dark'}
	title={currentTheme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
>
	<span class="theme-toggle-icon" aria-hidden="true">
		{#if currentTheme === 'dark'}
			<!-- Sun icon for switching to light -->
			<svg
				xmlns="http://www.w3.org/2000/svg"
				width="20"
				height="20"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
				stroke-linecap="round"
				stroke-linejoin="round"
			>
				<circle cx="12" cy="12" r="5"></circle>
				<line x1="12" y1="1" x2="12" y2="3"></line>
				<line x1="12" y1="21" x2="12" y2="23"></line>
				<line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line>
				<line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line>
				<line x1="1" y1="12" x2="3" y2="12"></line>
				<line x1="21" y1="12" x2="23" y2="12"></line>
				<line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line>
				<line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line>
			</svg>
		{:else}
			<!-- Moon icon for switching to dark -->
			<svg
				xmlns="http://www.w3.org/2000/svg"
				width="20"
				height="20"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
				stroke-linecap="round"
				stroke-linejoin="round"
			>
				<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path>
			</svg>
		{/if}
	</span>
	<span class="visually-hidden">
		{currentTheme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
	</span>
</button>

<style>
	.theme-toggle {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 40px;
		height: 40px;
		padding: var(--spacing-sm);
		background: transparent;
		border: 1px solid var(--color-border);
		border-radius: var(--border-radius-md);
		cursor: pointer;
		color: var(--color-text);
		transition:
			background-color var(--transition-fast),
			border-color var(--transition-fast),
			color var(--transition-fast);
	}

	.theme-toggle:hover {
		background-color: var(--color-surface);
		border-color: var(--color-primary);
	}

	.theme-toggle:focus-visible {
		outline: 2px solid var(--color-primary);
		outline-offset: 2px;
	}

	.theme-toggle-icon {
		display: flex;
		align-items: center;
		justify-content: center;
	}

	.theme-toggle-icon svg {
		transition: transform var(--transition-fast);
	}

	.theme-toggle:hover .theme-toggle-icon svg {
		transform: rotate(15deg);
	}

	.visually-hidden {
		position: absolute;
		width: 1px;
		height: 1px;
		padding: 0;
		margin: -1px;
		overflow: hidden;
		clip: rect(0, 0, 0, 0);
		white-space: nowrap;
		border: 0;
	}
</style>
