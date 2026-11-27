export default function InfoHover({ text }) {
  return (
    <span className="info" tabIndex={0} aria-label={text}>
      ?
      <span className="info-bubble" role="tooltip">{text}</span>
    </span>
  );
}
