// uba_frontend/src/components/ui/badge.jsx
export function Badge({ children, className = "" }) {
  return (
    <span className={`inline-flex items-center rounded-full border border-border px-2 py-0.5 text-xs ${className}`}>
      {children}
    </span>
  );
}
