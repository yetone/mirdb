# Configuration Reference

MirDB can be configured using a TOML configuration file. This page documents all available configuration parameters, their default values, and descriptions to help you tune MirDB for your specific workload.

<section id="configuration" class="configuration-section">

## Using Configuration Files

Start MirDB with a custom configuration file:

```bash
mirdb -c /path/to/config.toml
```

If no configuration file is specified, MirDB uses default values for all parameters.

</section>

<section id="configuration-table" class="configuration-parameters">

## Configuration Parameters

The following table lists all configurable parameters in MirDB:

<div class="config-table-container">
<table class="config-table" id="config-params-table">
  <thead>
    <tr>
      <th class="param-name">Parameter</th>
      <th class="param-default">Default Value</th>
      <th class="param-description">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr id="param-listen_addr">
      <td class="param-name"><code>listen_addr</code></td>
      <td class="param-default"><code>0.0.0.0:12333</code></td>
      <td class="param-description">Server listening address and port for client connections</td>
    </tr>
    <tr id="param-max_levels">
      <td class="param-name"><code>max_levels</code></td>
      <td class="param-default"><code>7</code></td>
      <td class="param-description">Maximum number of LSM tree levels for organizing SSTables</td>
    </tr>
    <tr id="param-work_dir">
      <td class="param-name"><code>work_dir</code></td>
      <td class="param-default"><code>/tmp/mirdb</code></td>
      <td class="param-description">Working directory for data storage, WAL files, and SSTables</td>
    </tr>
    <tr id="param-sstable_max_size">
      <td class="param-name"><code>sstable_max_size</code></td>
      <td class="param-default"><code>100MB</code></td>
      <td class="param-description">Maximum size per SSTable file before splitting</td>
    </tr>
    <tr id="param-memtable_max_size">
      <td class="param-name"><code>memtable_max_size</code></td>
      <td class="param-default"><code>4MB</code></td>
      <td class="param-description">Maximum memtable size before flushing to disk</td>
    </tr>
    <tr id="param-block_size">
      <td class="param-name"><code>block_size</code></td>
      <td class="param-default"><code>4KB</code></td>
      <td class="param-description">SSTable data block size for read/write operations</td>
    </tr>
  </tbody>
</table>
</div>

</section>

<section id="example-configuration" class="configuration-example">

## Example Configuration

Here is a complete example configuration file:

```toml
# Network Configuration
addr = "0.0.0.0:12333"

# LSM Tree Configuration
max_level = 7
work_dir = "/var/lib/mirdb"

# SSTable Settings
sst_max_size = "100M"
block_size = "4K"

# Memtable Settings
mem_table_max_size = "4M"
mem_table_max_height = 32
imm_mem_table_max_count = 16

# Compaction Settings
l0_compaction_trigger = 4
block_restart_interval = 16
thread_sleep_ms = 500
```

</section>

<section id="size-units" class="configuration-units">

## Size Units

Configuration supports the following size suffixes:

- **K** - Kilobytes (1024 bytes)
- **M** - Megabytes (1024 KB)
- **G** - Gigabytes (1024 MB)
- **T** - Terabytes (1024 GB)

</section>
