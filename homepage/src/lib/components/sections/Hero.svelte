<!--
  Hero Section Component
  Owner: Scenario 1 - Hero Section & Branding

  Displays:
  - MirDB ASCII logo
  - Product tagline: "A Persistent Key-Value Store with Memcached protocol"
  - Primary CTA button (Quick Start / Get Started)
  - GitHub stars badge

  REQ-1, REQ-2 traceability
-->
<script lang="ts">
	import { ASCII_LOGO, TAGLINE, GITHUB_URL } from '$lib/utils/constants.js';

	function scrollToQuickStart() {
		const quickStartSection = document.getElementById('quickstart');
		if (quickStartSection) {
			quickStartSection.scrollIntoView({ behavior: 'smooth' });
		}
	}
</script>

<section id="hero" class="hero" aria-label="Hero section">
	<div class="hero-content">
		<pre class="ascii-logo" aria-label="MirDB ASCII Logo">{ASCII_LOGO}</pre>
		<h1 class="title">MirDB</h1>
		<p class="tagline">{TAGLINE}</p>
		<div class="cta-container">
			<button type="button" class="cta-button primary" onclick={scrollToQuickStart}>
				Get Started
			</button>
			<a href={GITHUB_URL} target="_blank" rel="noopener noreferrer" class="cta-button secondary">
				View on GitHub
			</a>
		</div>
	</div>
</section>

<style>
	.hero {
		min-height: 100vh;
		display: flex;
		align-items: center;
		justify-content: center;
		padding: var(--spacing-2xl) var(--spacing-lg);
		background: linear-gradient(
			135deg,
			var(--color-bg) 0%,
			var(--color-bg-secondary) 100%
		);
	}

	.hero-content {
		text-align: center;
		max-width: 800px;
	}

	.ascii-logo {
		font-family: var(--font-family-mono);
		font-size: clamp(0.35rem, 1.5vw, 0.75rem);
		line-height: 1.2;
		color: var(--color-primary);
		margin-bottom: var(--spacing-lg);
		white-space: pre;
		overflow-x: auto;
	}

	.title {
		font-size: var(--font-size-5xl);
		font-weight: 700;
		color: var(--color-text);
		margin-bottom: var(--spacing-md);
	}

	.tagline {
		font-size: var(--font-size-xl);
		color: var(--color-text-secondary);
		margin-bottom: var(--spacing-xl);
		max-width: 600px;
		margin-left: auto;
		margin-right: auto;
	}

	.cta-container {
		display: flex;
		gap: var(--spacing-md);
		justify-content: center;
		flex-wrap: wrap;
	}

	.cta-button {
		padding: var(--spacing-md) var(--spacing-xl);
		font-size: var(--font-size-lg);
		font-weight: 600;
		border-radius: var(--radius-md);
		border: 2px solid transparent;
		transition: all var(--transition-fast);
		text-decoration: none;
		display: inline-block;
	}

	.cta-button.primary {
		background-color: var(--color-primary);
		color: var(--color-white);
		border-color: var(--color-primary);
	}

	.cta-button.primary:hover {
		background-color: var(--color-primary-dark);
		border-color: var(--color-primary-dark);
	}

	.cta-button.secondary {
		background-color: transparent;
		color: var(--color-text);
		border-color: var(--color-border);
	}

	.cta-button.secondary:hover {
		border-color: var(--color-primary);
		color: var(--color-primary);
	}

	@media (max-width: 768px) {
		.ascii-logo {
			font-size: 0.3rem;
		}

		.title {
			font-size: var(--font-size-4xl);
		}

		.tagline {
			font-size: var(--font-size-lg);
		}

		.cta-container {
			flex-direction: column;
			align-items: center;
		}

		.cta-button {
			width: 100%;
			max-width: 280px;
		}
	}
</style>
