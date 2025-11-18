export function Select({ value, onChange, children, className = "" }) {
  return (
    <select
      value={value}
      onChange={onChange}
      className={`bg-zinc-900 border border-border rounded-md px-3 py-2 text-sm ${className}`}
    >
      {children}
    </select>
  );
}
