<script lang="ts">
	import { Hero, About } from '$lib/components/index.js';
	import Features from '$lib/components/sections/Features.svelte';
</script>

<main>
	<Hero />
	<About />

	<!-- Features section -->
	<Features />

	<!-- QuickStart section placeholder for CTA scroll target -->
	<section id="quickstart" aria-label="Quick Start">
		<div class="container placeholder">
			<h2>Quick Start</h2>
			<p>Installation instructions will be added here by Scenario 4.</p>
		</div>
	</section>

	<!-- Roadmap section placeholder -->
	<section id="roadmap" class="section">
		<div class="container">
			<h2 class="section-title">Roadmap</h2>
		</div>
	</section>
</main>

<style>
	main {
		min-height: 100vh;
	}

	.container {
		max-width: 1200px;
		margin: 0 auto;
		padding: var(--spacing-3xl) var(--spacing-lg);
	}

	.section {
		padding: var(--spacing-3xl) 0;
	}

	.section-title {
		font-size: var(--font-size-3xl);
		color: var(--color-text);
		margin-bottom: var(--spacing-xl);
		text-align: center;
	}

	.placeholder {
		text-align: center;
	}

	.placeholder h2 {
		font-size: var(--font-size-3xl);
		color: var(--color-text);
		margin-bottom: var(--spacing-md);
	}

	.placeholder p {
		color: var(--color-text-secondary);
	}
</style>
