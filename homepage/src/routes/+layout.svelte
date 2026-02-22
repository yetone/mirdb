<script lang="ts">
	import '../styles/global.css';

	let { children } = $props();
</script>

<svelte:head>
	<title>MirDB - Persistent Key-Value Store</title>
</svelte:head>

{@render children()}
