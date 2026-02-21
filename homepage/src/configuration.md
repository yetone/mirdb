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

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| listen_addr | 0.0.0.0:12333 | Server listening address and port for client connections |
| max_levels | 7 | Maximum number of LSM tree levels for organizing SSTables |
| work_dir | /tmp/mirdb | Working directory for data storage, WAL files, and SSTables |
| sstable_max_size | 100MB | Maximum size per SSTable file before splitting |
| memtable_max_size | 4MB | Maximum memtable size before flushing to disk |
| block_size | 4KB | SSTable data block size for read/write operations |

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
