import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { Components } from "react-markdown";

/**
 * Markdown renderer for agent messages, themed to the AWS dark palette.
 * Safe by default — react-markdown does NOT render raw HTML (no rehype-raw),
 * so agent output can't inject markup. Tuned for chat density (tight spacing,
 * 13.5px base) and partial/streaming markdown (re-parses on each token).
 */
const components: Components = {
  p: ({ children }) => <p className="my-1.5 first:mt-0 last:mb-0">{children}</p>,
  strong: ({ children }) => (
    <strong className="font-semibold text-text-hi">{children}</strong>
  ),
  em: ({ children }) => <em className="italic">{children}</em>,
  a: ({ children, href }) => (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className="text-aws-blue underline decoration-aws-blue/40 underline-offset-2 hover:decoration-aws-blue"
    >
      {children}
    </a>
  ),
  h1: ({ children }) => (
    <h1 className="mt-3 mb-1.5 text-[15px] font-semibold text-text-hi first:mt-0">{children}</h1>
  ),
  h2: ({ children }) => (
    <h2 className="mt-3 mb-1.5 text-[14px] font-semibold text-text-hi first:mt-0">{children}</h2>
  ),
  h3: ({ children }) => (
    <h3 className="mt-2.5 mb-1 text-[13.5px] font-semibold text-text-hi first:mt-0">{children}</h3>
  ),
  ul: ({ children }) => (
    <ul className="my-1.5 ml-1 list-disc space-y-1 pl-4 marker:text-aws-orange/70">{children}</ul>
  ),
  ol: ({ children }) => (
    <ol className="my-1.5 ml-1 list-decimal space-y-1 pl-4 marker:text-text-lo">{children}</ol>
  ),
  li: ({ children }) => <li className="pl-0.5 leading-relaxed">{children}</li>,
  code: ({ className, children }) => {
    // Block code carries a language-* class; inline code does not.
    const isBlock = (className ?? "").includes("language-");
    if (isBlock) {
      return (
        <code className="block whitespace-pre overflow-x-auto rounded-lg border border-white/10 bg-black/40 p-2.5 font-mono text-[12px] text-text-hi">
          {children}
        </code>
      );
    }
    return (
      <code className="rounded bg-black/40 px-1 py-0.5 font-mono text-[0.85em] text-aws-orange">
        {children}
      </code>
    );
  },
  pre: ({ children }) => <pre className="my-2">{children}</pre>,
  blockquote: ({ children }) => (
    <blockquote className="my-2 border-l-2 border-aws-orange/50 pl-3 text-text-lo italic">
      {children}
    </blockquote>
  ),
  hr: () => <hr className="my-2.5 border-white/10" />,
  table: ({ children }) => (
    <div className="my-2 overflow-x-auto">
      <table className="w-full border-collapse text-[12.5px]">{children}</table>
    </div>
  ),
  th: ({ children }) => (
    <th className="border border-white/10 bg-white/5 px-2 py-1 text-left font-semibold text-text-hi">
      {children}
    </th>
  ),
  td: ({ children }) => (
    <td className="border border-white/10 px-2 py-1 align-top">{children}</td>
  ),
};

export function Markdown({ text }: { text: string }) {
  return (
    <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
      {text}
    </ReactMarkdown>
  );
}
