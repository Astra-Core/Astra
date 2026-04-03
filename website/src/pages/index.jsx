import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import styles from './index.module.css';

const features = [
  {
    title: 'Rust-first Performance',
    description:
      'Built in Rust from the ground up. Zero-overhead CDC, chunked JSONL.gz staging, and resumable checkpoints mean faster syncs with less infrastructure.',
  },
  {
    title: 'Declarative YAML Pipelines',
    description:
      'Define pipelines in version-controlled YAML (v1alpha1 spec). The same config drives the CLI, API, and web UI — no drift between interfaces.',
  },
  {
    title: 'Self-Hostable',
    description:
      'Run with a single Docker Compose command. No Kafka, no Kubernetes, no managed cloud required. Postgres for metadata, S3/MinIO for staging.',
  },
  {
    title: 'Resumable by Default',
    description:
      'Every snapshot is checkpointed. Kill a run mid-flight and restart — it picks up from the last committed chunk, not from the beginning.',
  },
  {
    title: 'Control Plane + Web UI',
    description:
      'REST API and React dashboard for pipeline inventory, run history, table-level drill-down, and an inline YAML editor — all served from one binary.',
  },
  {
    title: 'Extensible Connector Model',
    description:
      'Rust-native connectors for core sources (Postgres). Python subprocess runtime planned for long-tail SaaS connectors without polluting the hot path.',
  },
];

function Feature({ title, description }) {
  return (
    <div className={clsx('col col--4', styles.feature)}>
      <div className="feature-card">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/getting-started/quickstart">
            Get started in 15 minutes
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro"
            style={{ marginLeft: '1rem' }}>
            Read the docs
          </Link>
        </div>
        <div className={styles.codeSnippet}>
          <pre>
            <code>{`# Start the local stack
podman compose -f deploy/docker-compose/docker-compose.yml up -d

# Run your first snapshot
cargo run -p astra -- snapshot-to-local-staging pipeline.astra.yaml`}</code>
          </pre>
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  return (
    <Layout
      title="Self-hostable Data Replication"
      description="Astra is a Rust-first, self-hostable alternative to Airbyte and Fivetran for database CDC and bulk snapshot replication.">
      <HomepageHeader />
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {features.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
        <section className={styles.statusSection}>
          <div className="container">
            <div className={styles.statusBox}>
              <h2>v0.1 Status</h2>
              <div className={styles.statusGrid}>
                <div>
                  <h4>Working now</h4>
                  <ul>
                    <li>YAML pipeline spec (v1alpha1)</li>
                    <li>Postgres schema discovery</li>
                    <li>Full &amp; incremental snapshot</li>
                    <li>Local + MinIO staging (JSONL.gz)</li>
                    <li>Resumable checkpoint ledger</li>
                    <li>Postgres raw destination loader</li>
                    <li>Control-plane REST API (10 endpoints)</li>
                    <li>React web UI with YAML studio</li>
                  </ul>
                </div>
                <div>
                  <h4>Coming next</h4>
                  <ul>
                    <li>Postgres log-based CDC</li>
                    <li>Merge / upsert write mode</li>
                    <li>Snowflake &amp; BigQuery destinations</li>
                    <li>Python connector runtime</li>
                    <li>Distributed worker pool</li>
                    <li>Schema evolution enforcement</li>
                    <li>Secrets management (Vault, file)</li>
                    <li>Structured observability</li>
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
