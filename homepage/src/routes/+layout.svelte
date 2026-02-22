<script lang="ts">
	import Header from '$lib/components/layout/Header.svelte';
	import Footer from '$lib/components/layout/Footer.svelte';
	import ThemeToggle from '$lib/components/ui/ThemeToggle.svelte';
	import { initTheme } from '$lib/stores/theme';
	import { onMount } from 'svelte';
	import '../styles/global.css';

	let { children } = $props();

	onMount(() => {
		initTheme();
	});
</script>

<svelte:head>
	<title>MirDB - Persistent Key-Value Store with Memcached Protocol</title>
	<meta name="description" content="MirDB is a persistent key-value store compatible with Memcached protocol, built with Rust for high performance and reliability." />
</svelte:head>

<a href="#main-content" class="skip-link">Skip to main content</a>

<div class="header-wrapper">
	<Header />
	<div class="theme-toggle-container">
		<ThemeToggle />
	</div>
</div>

<main id="main-content" class="main-content">
	{@render children()}
</main>

<Footer />

<style>
	.header-wrapper {
		position: relative;
	}

	.theme-toggle-container {
		position: fixed;
		top: calc((var(--header-height) - 40px) / 2);
		right: var(--spacing-lg);
		z-index: 101;
	}

	.main-content {
		margin-top: var(--header-height);
		min-height: calc(100vh - var(--header-height));
		display: flex;
		flex-direction: column;
	}

	@media (max-width: 768px) {
		.theme-toggle-container {
			right: calc(var(--spacing-lg) + 50px);
		}
	}
</style>
