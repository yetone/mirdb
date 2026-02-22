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

	<!-- Canonical URL -->
	<link rel="canonical" href="https://mirdb.io/" />

	<!-- Open Graph tags for social sharing -->
	<meta property="og:title" content="MirDB - Persistent Key-Value Store with Memcached Protocol" />
	<meta property="og:description" content="MirDB is a persistent key-value store compatible with Memcached protocol, built with Rust for high performance and reliability." />
	<meta property="og:image" content="https://mirdb.io/og-image.png" />
	<meta property="og:url" content="https://mirdb.io/" />
	<meta property="og:type" content="website" />
	<meta property="og:site_name" content="MirDB" />

	<!-- Twitter Card tags -->
	<meta name="twitter:card" content="summary_large_image" />
	<meta name="twitter:title" content="MirDB - Persistent Key-Value Store with Memcached Protocol" />
	<meta name="twitter:description" content="MirDB is a persistent key-value store compatible with Memcached protocol, built with Rust for high performance and reliability." />
	<meta name="twitter:image" content="https://mirdb.io/og-image.png" />

	<!-- Additional SEO meta tags -->
	<meta name="robots" content="index, follow" />
	<meta name="author" content="MirDB Team" />
	<meta name="keywords" content="MirDB, key-value store, memcached, persistent storage, Rust, LSM-tree, database" />
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
