<script lang="ts">
	import type { NavItem } from '$lib/types';

	interface Props {
		items: NavItem[];
		mobile?: boolean;
	}

	let { items, mobile = false }: Props = $props();

	function handleNavClick(event: MouseEvent, href: string) {
		if (href.startsWith('#')) {
			event.preventDefault();
			const target = document.querySelector(href);
			if (target) {
				target.scrollIntoView({ behavior: 'smooth' });
				window.history.pushState(null, '', href);
			}
		}
	}
</script>

<nav class="navigation" class:mobile aria-label="Main navigation">
	<ul class="nav-list">
		{#each items as item}
			<li class="nav-item">
				{#if item.external}
					<a
						href={item.href}
						target="_blank"
						rel="noopener noreferrer"
						class="nav-link"
						aria-label="{item.label} (opens in new tab)"
					>
						{item.label}
					</a>
				{:else}
					<a
						href={item.href}
						class="nav-link"
						onclick={(e) => handleNavClick(e, item.href)}
					>
						{item.label}
					</a>
				{/if}
			</li>
		{/each}
	</ul>
</nav>

<style>
	.navigation {
		display: flex;
		align-items: center;
	}

	.nav-list {
		display: flex;
		align-items: center;
		gap: var(--spacing-lg);
		list-style: none;
		margin: 0;
		padding: 0;
	}

	.nav-item {
		margin: 0;
	}

	.nav-link {
		display: inline-block;
		padding: var(--spacing-sm) var(--spacing-md);
		color: var(--color-text);
		font-weight: 500;
		text-decoration: none;
		border-radius: var(--border-radius-md);
		transition: color var(--transition-fast), background-color var(--transition-fast);
	}

	.nav-link:hover {
		color: var(--color-primary);
		background-color: var(--color-surface);
	}

	/* Mobile styles */
	.navigation.mobile {
		flex-direction: column;
		width: 100%;
	}

	.navigation.mobile .nav-list {
		flex-direction: column;
		width: 100%;
		gap: var(--spacing-sm);
	}

	.navigation.mobile .nav-item {
		width: 100%;
	}

	.navigation.mobile .nav-link {
		display: block;
		width: 100%;
		padding: var(--spacing-md);
		text-align: center;
	}

	@media (max-width: 768px) {
		.navigation:not(.mobile) {
			display: none;
		}
	}
</style>
