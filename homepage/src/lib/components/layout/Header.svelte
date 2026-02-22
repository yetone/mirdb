<script lang="ts">
	import Navigation from './Navigation.svelte';
	import { NAV_ITEMS } from '$lib/utils/constants';

	let mobileMenuOpen = $state(false);

	function toggleMobileMenu() {
		mobileMenuOpen = !mobileMenuOpen;
	}

	function closeMobileMenu() {
		mobileMenuOpen = false;
	}
</script>

<header class="header">
	<div class="header-container container">
		<a href="/" class="logo" aria-label="MirDB Home">
			<span class="logo-text">MirDB</span>
		</a>

		<Navigation items={NAV_ITEMS} />

		<button
			class="mobile-menu-toggle"
			onclick={toggleMobileMenu}
			aria-expanded={mobileMenuOpen}
			aria-controls="mobile-menu"
			aria-label={mobileMenuOpen ? 'Close menu' : 'Open menu'}
		>
			<span class="hamburger-icon" class:open={mobileMenuOpen}>
				<span class="line"></span>
				<span class="line"></span>
				<span class="line"></span>
			</span>
		</button>
	</div>

	{#if mobileMenuOpen}
		<div
			id="mobile-menu"
			class="mobile-menu"
			role="dialog"
			aria-modal="true"
			aria-label="Mobile navigation"
		>
			<Navigation items={NAV_ITEMS} mobile={true} onnavigate={closeMobileMenu} />
		</div>
		<button
			class="mobile-menu-overlay"
			onclick={closeMobileMenu}
			aria-label="Close menu"
		></button>
	{/if}
</header>

<style>
	.header {
		position: fixed;
		top: 0;
		left: 0;
		right: 0;
		z-index: 100;
		height: var(--header-height);
		background-color: var(--color-background);
		border-bottom: 1px solid var(--color-border);
		backdrop-filter: blur(8px);
	}

	.header-container {
		display: flex;
		align-items: center;
		justify-content: space-between;
		height: 100%;
	}

	.logo {
		display: flex;
		align-items: center;
		text-decoration: none;
		color: var(--color-text);
		min-height: 44px;
		min-width: 44px;
	}

	.logo-text {
		font-size: var(--font-size-xl);
		font-weight: 700;
		color: var(--color-primary);
	}

	.mobile-menu-toggle {
		display: none;
		padding: var(--spacing-sm);
		background: none;
		border: none;
		cursor: pointer;
		min-width: 44px;
		min-height: 44px;
		align-items: center;
		justify-content: center;
	}

	.hamburger-icon {
		display: flex;
		flex-direction: column;
		justify-content: space-between;
		width: 24px;
		height: 18px;
	}

	.hamburger-icon .line {
		display: block;
		width: 100%;
		height: 2px;
		background-color: var(--color-text);
		border-radius: 1px;
		transition: transform var(--transition-fast), opacity var(--transition-fast);
	}

	.hamburger-icon.open .line:nth-child(1) {
		transform: translateY(8px) rotate(45deg);
	}

	.hamburger-icon.open .line:nth-child(2) {
		opacity: 0;
	}

	.hamburger-icon.open .line:nth-child(3) {
		transform: translateY(-8px) rotate(-45deg);
	}

	.mobile-menu {
		position: fixed;
		top: var(--header-height);
		left: 0;
		right: 0;
		background-color: var(--color-background);
		border-bottom: 1px solid var(--color-border);
		padding: var(--spacing-md);
		z-index: 99;
	}

	.mobile-menu-overlay {
		position: fixed;
		top: 0;
		left: 0;
		right: 0;
		bottom: 0;
		background-color: rgba(0, 0, 0, 0.5);
		z-index: 98;
		border: none;
		cursor: pointer;
	}

	@media (max-width: 768px) {
		.mobile-menu-toggle {
			display: flex;
		}
	}
</style>
