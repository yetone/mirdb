import React from 'react';

const ConfigurationSection = () => {
  const configValues = [
    { setting: 'Listen address', value: '0.0.0.0:12333' },
    { setting: 'Max LSM levels', value: '7' },
    { setting: 'Work directory', value: '/tmp/mirdb' },
    { setting: 'SSTable max size', value: '100MB' },
    { setting: 'Memtable max size', value: '4MB' },
    { setting: 'Block size', value: '4KB' },
  ];

  return (
    <section id="configuration" className="section">
      <div className="container">
        <h2 className="section-title">Configuration Reference</h2>
        <div className="table-wrapper">
          <table className="config-table">
            <thead>
              <tr>
                <th>Setting</th>
                <th>Default Value</th>
              </tr>
            </thead>
            <tbody>
              {configValues.map((item, index) => (
                <tr key={index}>
                  <td className="config-setting">{item.setting}</td>
                  <td className="config-value">{item.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
};

export default ConfigurationSection;