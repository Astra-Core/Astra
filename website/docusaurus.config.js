// @ts-check

const { themes: prismThemes } = require('prism-react-renderer');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Astra',
  tagline: 'Self-hostable data replication — a Rust-first alternative to Airbyte & Fivetran',
  favicon: 'img/favicon.ico',

  url: 'https://astra-core.github.io',
  baseUrl: '/Astra/',

  organizationName: 'astra-core',
  projectName: 'Astra',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/astra-core/astra-core.github.io/edit/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/astra-social-card.png',
      colorMode: {
        defaultMode: 'dark',
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Astra',
        logo: {
          alt: 'Astra Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            href: 'https://github.com/astra-core/astra-core.github.io',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Quickstart', to: '/docs/getting-started/quickstart' },
              { label: 'YAML Spec', to: '/docs/yaml-spec/overview' },
              { label: 'CLI Reference', to: '/docs/cli/reference' },
              { label: 'API Reference', to: '/docs/control-plane/api' },
            ],
          },
          {
            title: 'Project',
            items: [
              { label: 'GitHub', href: 'https://github.com/astra-core/astra-core.github.io' },
              { label: 'Changelog', href: 'https://github.com/astra-core/astra-core.github.io/blob/main/CHANGELOG.md' },

              { label: 'Contributing', to: '/docs/development/contributing' },
            ],
          },
          {
            title: 'Architecture',
            items: [
              { label: 'Overview', to: '/docs/architecture/overview' },
              { label: 'ADR-0001: Modular Monolith', to: '/docs/architecture/modular-monolith' },
              { label: 'Roadmap', to: '/docs/roadmap' },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Astra Contributors. Licensed under Apache 2.0.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['rust', 'toml', 'yaml', 'bash', 'sql'],
      },
      algolia: undefined,
    }),
};

module.exports = config;
