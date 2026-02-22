<!--
  Feature Card Component
  Owner: Scenario 3 - Features Section

  Props:
  - icon: string (icon identifier)
  - title: string
  - description: string

  Displays individual feature with icon and description
-->
<script lang="ts">
	export let icon: string;
	export let title: string;
	export let description: string;
</script>

<article class="feature-card" data-testid="feature-card">
	<div class="feature-icon" aria-hidden="true">{icon}</div>
	<h3 class="feature-title">{title}</h3>
	<p class="feature-description">{description}</p>
</article>

<style>
	.feature-card {
		background-color: var(--color-surface);
		border: 1px solid var(--color-border);
		border-radius: var(--radius-lg);
		padding: var(--spacing-6);
		transition: transform var(--transition-fast), box-shadow var(--transition-fast);
	}

	.feature-card:hover {
		transform: translateY(-2px);
		box-shadow: var(--shadow-md);
	}

	.feature-icon {
		font-size: var(--font-size-4xl);
		margin-bottom: var(--spacing-4);
	}

	.feature-title {
		font-size: var(--font-size-xl);
		font-weight: 600;
		margin-bottom: var(--spacing-2);
		color: var(--color-text);
	}

	.feature-description {
		font-size: var(--font-size-base);
		color: var(--color-text-muted);
		line-height: 1.6;
	}
</style>
