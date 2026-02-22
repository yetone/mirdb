<script lang="ts">
	import { ASCII_LOGO, FEATURES, GITHUB_URL } from '$lib/utils/constants';
	import QuickStart from '$lib/components/sections/QuickStart.svelte';
</script>

<!-- Hero Section -->
<section id="hero" class="section hero">
	<div class="container">
		<pre class="ascii-logo" aria-label="MirDB ASCII Logo">{ASCII_LOGO}</pre>
		<h1>MirDB</h1>
		<p class="tagline">A Persistent Key-Value Store with Memcached protocol</p>
		<div class="cta-buttons">
			<a href="#quickstart" class="btn btn-primary">Get Started</a>
			<a href={GITHUB_URL} target="_blank" rel="noopener noreferrer" class="btn btn-secondary">
				View on GitHub
			</a>
		</div>
	</div>
</section>

<!-- Features Section -->
<section id="features" class="section features">
	<div class="container">
		<h2>Features</h2>
		<div class="feature-grid">
			{#each FEATURES as feature}
				<div class="feature-card">
					<span class="feature-icon">{feature.icon}</span>
					<h3>{feature.title}</h3>
					<p>{feature.description}</p>
				</div>
			{/each}
		</div>
	</div>
</section>

<!-- Quick Start Section -->
<QuickStart />

<!-- Docs Section -->
<section id="docs" class="section docs">
	<div class="container">
		<h2>Documentation</h2>
		<p>Comprehensive documentation is available on our <a href={GITHUB_URL} target="_blank" rel="noopener noreferrer">GitHub repository</a>.</p>
	</div>
</section>

<style>
	.hero {
		text-align: center;
		padding: var(--spacing-3xl) 0;
		background: linear-gradient(180deg, var(--color-surface) 0%, var(--color-background) 100%);
	}

	.ascii-logo {
		font-family: var(--font-family-mono);
		font-size: var(--font-size-sm);
		color: var(--color-primary);
		margin: 0 0 var(--spacing-lg);
		line-height: 1.2;
	}

	.tagline {
		font-size: var(--font-size-xl);
		color: var(--color-text-secondary);
		margin-bottom: var(--spacing-xl);
	}

	.cta-buttons {
		display: flex;
		gap: var(--spacing-md);
		justify-content: center;
		flex-wrap: wrap;
	}

	.btn {
		display: inline-flex;
		align-items: center;
		padding: var(--spacing-sm) var(--spacing-lg);
		border-radius: var(--border-radius-md);
		font-weight: 600;
		text-decoration: none;
		transition: all var(--transition-fast);
	}

	.btn-primary {
		background-color: var(--color-primary);
		color: white;
	}

	.btn-primary:hover {
		background-color: var(--color-primary-hover);
		color: white;
	}

	.btn-secondary {
		background-color: var(--color-surface);
		color: var(--color-text);
		border: 1px solid var(--color-border);
	}

	.btn-secondary:hover {
		background-color: var(--color-border);
	}

	.features {
		background-color: var(--color-background);
	}

	.features h2 {
		text-align: center;
		margin-bottom: var(--spacing-2xl);
	}

	.feature-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
		gap: var(--spacing-lg);
	}

	.feature-card {
		background-color: var(--color-surface);
		padding: var(--spacing-lg);
		border-radius: var(--border-radius-lg);
		border: 1px solid var(--color-border);
	}

	.feature-icon {
		font-size: var(--font-size-2xl);
		margin-bottom: var(--spacing-sm);
		display: block;
	}

	.feature-card h3 {
		margin-bottom: var(--spacing-sm);
	}

	.feature-card p {
		color: var(--color-text-secondary);
		margin: 0;
	}

	.docs h2 {
		text-align: center;
		margin-bottom: var(--spacing-xl);
	}

	.docs {
		text-align: center;
	}

	@media (max-width: 768px) {
		.ascii-logo {
			font-size: 0.6rem;
		}
	}
</style>
