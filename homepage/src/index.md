<!--
MirDB Homepage Main Content
Owners:
  - Scenario 1: Hero section
  - Scenario 3: Features section
  - Scenario 4: Quick Start section
  - Scenario 6: Roadmap section

Expected sections (in order):
1. Hero - Tagline, logo, Get Started CTA
2. Features - Grid of core capabilities
3. Quick Start - Installation and usage code
4. Roadmap - Completed and planned features
-->

<!-- ===== HERO SECTION - Scenario 1 ===== -->
<section id="hero" class="hero">
  <div class="hero-content">
    <img src="images/logo.gif" alt="MirDB Logo" class="hero-logo" id="mirdb-logo">
    <h1 class="hero-title">MirDB</h1>
    <p class="hero-tagline">A Persistent Key-Value Store with Memcached Protocol</p>
    <a href="#quickstart" class="cta-button" id="get-started-btn">Get Started</a>
  </div>
</section>
<!-- ===== END HERO SECTION ===== -->

<!-- ===== FEATURES SECTION - Scenario 3 ===== -->
<section id="features" class="features">
  <h2 class="features-title">Core Features</h2>
  <div class="features-grid">
    <div class="feature-card" id="feature-memcached">
      <h3 class="feature-card-title">Memcached Protocol Support</h3>
      <p class="feature-card-description">Full compatibility with the Memcached protocol allows seamless integration with existing clients and tools. Use any memcached client library to connect to MirDB.</p>
    </div>
    <div class="feature-card" id="feature-sstables">
      <h3 class="feature-card-title">Durability with SSTables</h3>
      <p class="feature-card-description">Data is persisted to disk using Sorted String Tables (SSTables), ensuring your data survives restarts and crashes with configurable durability guarantees.</p>
    </div>
    <div class="feature-card" id="feature-lsm">
      <h3 class="feature-card-title">LSM Tree Architecture</h3>
      <p class="feature-card-description">Log-Structured Merge Tree architecture provides optimal write performance with efficient compaction strategies, including minor and major compaction support.</p>
    </div>
    <div class="feature-card" id="feature-rust">
      <h3 class="feature-card-title">High-Performance Rust Implementation</h3>
      <p class="feature-card-description">Built with Rust for memory safety and performance. Leverages Tokio for async networking and provides zero-copy operations where possible.</p>
    </div>
  </div>
</section>
<!-- ===== END FEATURES SECTION ===== -->

<!-- ===== QUICK START SECTION - Scenario 4 ===== -->
<section id="quickstart" class="quickstart">
  <h2>Quick Start</h2>
  <p class="quickstart-intro">Get MirDB running in minutes. Install via Cargo and start storing key-value pairs using the familiar Memcached protocol.</p>

  <div class="quickstart-step">
    <h3>1. Install MirDB</h3>
    <div class="code-block" data-language="bash">
      <button class="copy-btn" aria-label="Copy to clipboard" title="Copy to clipboard">
        <span class="copy-icon">📋</span>
        <span class="copy-text">Copy</span>
      </button>
      <pre><code class="language-bash">cargo install mirdb</code></pre>
    </div>
  </div>

  <div class="quickstart-step">
    <h3>2. Start the Server</h3>
    <div class="code-block" data-language="bash">
      <button class="copy-btn" aria-label="Copy to clipboard" title="Copy to clipboard">
        <span class="copy-icon">📋</span>
        <span class="copy-text">Copy</span>
      </button>
      <pre><code class="language-bash">mirdb-server --addr 0.0.0.0:12333 --dir /tmp/mirdb</code></pre>
    </div>
  </div>

  <div class="quickstart-step">
    <h3>3. Connect and Use</h3>
    <p>Connect using any Memcached client or telnet:</p>
    <div class="code-block" data-language="bash">
      <button class="copy-btn" aria-label="Copy to clipboard" title="Copy to clipboard">
        <span class="copy-icon">📋</span>
        <span class="copy-text">Copy</span>
      </button>
      <pre><code class="language-bash">telnet localhost 12333</code></pre>
    </div>
  </div>

  <div class="quickstart-step">
    <h3>4. Store and Retrieve Data</h3>
    <p>Use standard Memcached protocol commands:</p>
    <div class="code-block" data-language="text">
      <button class="copy-btn" aria-label="Copy to clipboard" title="Copy to clipboard">
        <span class="copy-icon">📋</span>
        <span class="copy-text">Copy</span>
      </button>
      <pre><code class="language-memcached"><span class="token-comment"># Store a value</span>
<span class="token-command">set mykey 0 0 5</span>
hello
<span class="token-response">STORED</span>
<span class="token-comment"># Retrieve the value</span>
<span class="token-command">get mykey</span>
<span class="token-response">VALUE mykey 0 5
hello
END</span></code></pre>
    </div>
  </div>
</section>
<!-- ===== END QUICK START SECTION ===== -->

<!-- Roadmap Section - Scenario 6 -->
<section id="roadmap" class="roadmap">
  <!-- Roadmap content will be added by Scenario 6 -->
</section>
