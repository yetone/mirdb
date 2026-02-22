<script lang="ts">
	import Header from '$lib/components/layout/Header.svelte';
	import Footer from '$lib/components/layout/Footer.svelte';
	import '../styles/global.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>MirDB - Persistent Key-Value Store with Memcached Protocol</title>
	<meta name="description" content="MirDB is a persistent key-value store compatible with Memcached protocol, built with Rust for high performance and reliability." />
</svelte:head>

<a href="#main-content" class="skip-link">Skip to main content</a>

<Header />

<main id="main-content" class="main-content">
	{@render children()}
</main>

<Footer />

<style>
	.main-content {
		margin-top: var(--header-height);
		min-height: calc(100vh - var(--header-height));
		display: flex;
		flex-direction: column;
	}
</style>
