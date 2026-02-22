<!--
  Features Section Component
  Owner: Scenario 3 - Features Section

  Displays grid of feature cards:
  - Memcached Compatible
  - Persistent Storage
  - LSM-Tree Architecture
  - Rust Implementation
  - Write-Ahead Logging
  - Compaction Support

  Uses: FeatureCard component

  REQ-4 traceability
-->
<script lang="ts">
	import FeatureCard from '$lib/components/ui/FeatureCard.svelte';
	import { FEATURES } from '$lib/utils/constants';
</script>

<section id="features" class="features-section section" aria-labelledby="features-heading">
	<div class="container">
		<h2 id="features-heading" class="section-title">Key Features</h2>
		<div class="features-grid">
			{#each FEATURES as feature}
				<FeatureCard
					icon={feature.icon}
					title={feature.title}
					description={feature.description}
				/>
			{/each}
		</div>
	</div>
</section>

<style>
	.features-section {
		background-color: var(--color-background);
	}

	.features-grid {
		display: grid;
		grid-template-columns: repeat(3, 1fr);
		gap: var(--spacing-6);
	}

	@media (max-width: 992px) {
		.features-grid {
			grid-template-columns: repeat(2, 1fr);
		}
	}

	@media (max-width: 576px) {
		.features-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
