import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'OSO CDC Connector for Oracle Database',
  tagline: 'Open-source change data capture from Oracle Database to Apache Kafka through LogMiner',
  favicon: 'img/oso-favicon.svg',
  url: process.env.DOCS_URL || 'https://kafkacdcconnector.com',
  baseUrl: process.env.DOCS_BASE_URL || '/',

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      ({
        hashed: true,
        docsRouteBasePath: '/',
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        searchResultLimits: 10,
        explicitSearchResultPath: true,
      }),
    ],
  ],

  presets: [
    [
      'classic',
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/osodevops/kafka-connect-oracle-cdc/tree/main/website/',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.5,
        },
      }),
    ],
  ],

  themeConfig: ({
    navbar: {
      title: 'OSO CDC Connector for Oracle Database',
      logo: {
        alt: 'OSO Logo',
        src: 'img/oso-logo.svg',
      },
      items: [
        {type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs'},
        {type: 'search', position: 'right'},
        {href: 'https://github.com/osodevops/kafka-connect-oracle-cdc', label: 'GitHub', position: 'right'},
        {href: 'https://www.oso.sh', label: 'OSO', position: 'right'},
        {to: '/enterprise-support', label: 'Enterprise Support', position: 'right'},
        {href: 'https://oso.sh/contact/', label: 'Contact', position: 'right'},
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Getting started', to: '/getting-started'},
            {label: 'Database setup', to: '/database-setup'},
            {label: 'oracle-cdc-doctor', to: '/operations/doctor'},
            {label: 'Migration', to: '/migration/from-debezium'},
            {label: 'Enterprise support', to: '/enterprise-support'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/osodevops/kafka-connect-oracle-cdc'},
            {label: 'Issues', href: 'https://github.com/osodevops/kafka-connect-oracle-cdc/issues'},
            {label: 'Discussions', href: 'https://github.com/osodevops/kafka-connect-oracle-cdc/discussions'},
          ],
        },
        {
          title: 'OSO',
          items: [
            {label: 'oso.sh', href: 'https://www.oso.sh'},
            {label: 'Salesforce Kafka Connector', href: 'https://salesforcekafkaconnector.com'},
            {label: 'Kafka Backup', href: 'https://kafkabackup.com'},
            {label: 'Contact', href: 'https://oso.sh/contact/'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} OSO DevOps Ltd. Built with Docusaurus.<br/>
        <small>This is an independent open-source project and is not affiliated with, endorsed by or
        sponsored by Oracle Corporation or the Apache Software Foundation. Oracle, Java and MySQL are
        registered trademarks of Oracle and/or its affiliates. Apache, <a href="https://kafka.apache.org">Apache Kafka</a>
        and Kafka are trademarks of the Apache Software Foundation. Other names may be trademarks of
        their respective owners.</small>`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'yaml', 'json', 'java', 'properties', 'sql'],
    },
  }),
};

export default config;
