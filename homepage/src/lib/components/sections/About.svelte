<!--
  About Section Component
  Owner: Scenario 1 - Hero Section & Branding

  Brief explanation of what MirDB is:
  - Persistent key-value store
  - Memcached protocol compatibility
  - Use cases

  REQ-2 traceability
-->
<script lang="ts">
</script>

<section id="about" class="about" aria-label="About MirDB">
	<div class="container">
		<h2 class="section-title">What is MirDB?</h2>
		<div class="about-content">
			<p>
				MirDB is a <strong>persistent key-value store</strong> written in Rust that implements
				the Memcached protocol. Unlike traditional memcached which stores data in memory only,
				MirDB persists your data to disk using SSTables (Sorted String Tables).
			</p>
			<p>
				This means you get the familiar Memcached interface you know, with the added benefit
				of <strong>durability</strong> and <strong>crash recovery</strong>. Your existing
				Memcached clients work seamlessly with MirDB.
			</p>
		</div>
	</div>
</section>

<style>
	.about {
		padding: var(--spacing-3xl) var(--spacing-lg);
		background-color: var(--color-bg-secondary);
	}

	.container {
		max-width: 800px;
		margin: 0 auto;
	}

	.section-title {
		font-size: var(--font-size-3xl);
		color: var(--color-text);
		margin-bottom: var(--spacing-xl);
		text-align: center;
	}

	.about-content {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-lg);
	}

	.about-content p {
		font-size: var(--font-size-lg);
		color: var(--color-text-secondary);
		line-height: 1.8;
	}

	.about-content strong {
		color: var(--color-primary);
	}
</style>
