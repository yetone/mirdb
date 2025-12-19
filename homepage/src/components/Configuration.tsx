import React from 'react';

export interface ConfigParameter {
  name: string;
  description: string;
  default: string;
  category: string;
}

export const configParametersData: ConfigParameter[] = [
  {
    name: 'addr',
    description: 'Listen address and port for the server',
    default: '0.0.0.0:12333',
    category: 'Network',
  },
  {
    name: 'work_dir',
    description: 'Data directory path for storing database files',
    default: '/tmp/mirdb',
    category: 'Storage',
  },
  {
    name: 'max_level',
    description: 'Maximum LSM tree levels',
    default: '7',
    category: 'Storage',
  },
  {
    name: 'mem_table_max_size',
    description: 'Maximum memtable size before flush',
    default: '4M',
    category: 'Memory Tables',
  },
  {
    name: 'sst_max_size',
    description: 'Maximum SSTable file size',
    default: '100M',
    category: 'SSTables',
  },
  {
    name: 'block_size',
    description: 'Data block size within SSTables',
    default: '4K',
    category: 'SSTables',
  },
  {
    name: 'l0_compaction_trigger',
    description: 'Number of Level 0 files before triggering compaction',
    default: '4',
    category: 'Compaction',
  },
];

export const exampleTomlConfig = `addr = "0.0.0.0:12333"

max_level = 7
work_dir = "/tmp/mirdb"

sst_max_size = "100M"
mem_table_max_size = "4M"`;

export const Configuration: React.FC = () => {
  return (
    <section id="configuration" className="configuration-section" data-testid="configuration-section">
      <div className="container">
        <h2 className="section-title">Configuration</h2>
        <p className="section-description">
          MirDB uses TOML configuration files. Specify your config file using the <code>-c</code> flag.
        </p>

        <h3>Example Configuration</h3>
        <pre className="code-block" data-testid="toml-config-block">
          <code className="language-toml" data-testid="toml-code">{exampleTomlConfig}</code>
        </pre>

        <h3>Configuration Parameters</h3>
        <div className="config-table-wrapper">
          <table className="config-table" data-testid="config-table">
            <thead>
              <tr>
                <th>Parameter</th>
                <th>Description</th>
                <th>Default</th>
              </tr>
            </thead>
            <tbody>
              {configParametersData.map((param) => (
                <tr key={param.name} data-testid={`config-param-${param.name}`}>
                  <td className="param-name">
                    <code>{param.name}</code>
                  </td>
                  <td className="param-description">{param.description}</td>
                  <td className="param-default">
                    <code>{param.default}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
};

export default Configuration;
