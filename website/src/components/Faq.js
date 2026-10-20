import React from 'react';
import Head from '@docusaurus/Head';

/**
 * Visible FAQ section for an MDX page, plus one FAQPage JSON-LD block built
 * from the same questions and answers.
 *
 * Usage in MDX: `import Faq from '@site/src/components/Faq';` then
 * `<Faq items={[...]} />`.
 *
 * Use this component at most once per page so each route has a single FAQPage.
 * Each item is `{question, answer}` and `answer` is plain text, so the
 * paragraph on the page and the structured-data text are the same string.
 *
 * @param {object} props
 * @param {{question: string, answer: string}[]} props.items
 * @param {string} [props.title] Visible H2. Defaults to "Frequently asked questions" with id `faq`.
 */
export default function Faq({items, title = 'Frequently asked questions'}) {
  const structuredData = {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: items.map(({question, answer}) => ({
      '@type': 'Question',
      name: question,
      acceptedAnswer: {
        '@type': 'Answer',
        text: answer,
      },
    })),
  };

  return (
    <section>
      <Head>
        <script type="application/ld+json">
          {JSON.stringify(structuredData)}
        </script>
      </Head>
      <h2 id="faq">{title}</h2>
      {items.map(({question, answer}) => (
        <div key={question}>
          <h3>{question}</h3>
          <p>{answer}</p>
        </div>
      ))}
    </section>
  );
}
