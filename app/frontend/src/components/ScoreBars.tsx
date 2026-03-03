interface Props {
  housing: number;
  lifestyle: number;
  spillover: number;
}

function Bar({ label, value, cls }: { label: string; value: number; cls: string }) {
  return (
    <div className="score-bar-item">
      <div className="score-bar-label">
        {label} {(value * 100).toFixed(0)}%
      </div>
      <div className="score-bar-track">
        <div
          className={`score-bar-fill ${cls}`}
          style={{ width: `${Math.min(value * 100, 100)}%` }}
        />
      </div>
    </div>
  );
}

export function ScoreBars({ housing, lifestyle, spillover }: Props) {
  return (
    <div className="score-bars">
      <Bar label="Housing" value={housing} cls="housing" />
      <Bar label="Lifestyle" value={lifestyle} cls="lifestyle" />
      <Bar label="Spillover" value={spillover} cls="spillover" />
    </div>
  );
}
