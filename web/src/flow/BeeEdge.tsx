/**
 * Bee edge (S06/R-U6): direction source -> target, bee glyph at the midpoint
 * oriented along the data flow. The bee is a routing marker, not decoration.
 */
import {
  BaseEdge,
  EdgeLabelRenderer,
  getStraightPath,
  type EdgeProps,
} from '@xyflow/react';
import { beeAngle } from '../a11y/keyboard';
import { useAppReducedMotion } from '../state/useAppReducedMotion';

export function BeeEdge(props: EdgeProps) {
  const reduced = useAppReducedMotion();
  const [path] = getStraightPath({
    sourceX: props.sourceX,
    sourceY: props.sourceY,
    targetX: props.targetX,
    targetY: props.targetY,
  });
  const labelX = (props.sourceX + props.targetX) / 2;
  const labelY = (props.sourceY + props.targetY) / 2;
  const angle = (beeAngle(
    { x: props.sourceX, y: props.sourceY },
    { x: props.targetX, y: props.targetY },
  ) * 180) /
    Math.PI;
  const broken = (props.data as { broken?: boolean } | undefined)?.broken === true;
  const flip = Math.abs(angle) > 90;
  const rideDelay = (props.id.length * 0.37) % 6;
  const supportsOffsetPath =
    typeof CSS !== 'undefined' &&
    typeof CSS.supports === 'function' &&
    CSS.supports('offset-path', 'path("M 0 0 L 1 1")');
  return (
    <>
      {/* A wide, low-opacity honey wash under the hairline route: gives the
          edge a lit trail without a filter or a second animated layer. */}
      <path
        className="bee-edge-glow"
        d={path}
        fill="none"
        stroke={broken ? 'var(--color-signal)' : 'var(--color-honey-300)'}
        strokeWidth="5"
        strokeLinecap="round"
        opacity="0.22"
        pointerEvents="none"
      />
      <BaseEdge
        id={props.id}
        path={path}
        markerEnd={broken ? 'url(#bee-arrow-broken)' : 'url(#bee-arrow)'}
        style={{
          stroke: broken ? 'var(--color-signal)' : 'var(--color-honey-400)',
          strokeWidth: 1.5,
          strokeDasharray: broken ? '6 6' : '10 10',
          animation: reduced
            ? undefined
            : `${broken ? 'bee-dash-broken' : 'bee-dash'} 2.4s linear infinite`,
        }}
      />
      {/* Pollen landing ring at the receiving end, pointing the route in. */}
      <EdgeLabelRenderer>
        <div
          className="bee-edge-landing"
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${props.targetX}px, ${props.targetY}px)`,
            pointerEvents: 'none',
            color: broken ? 'var(--color-signal)' : 'var(--color-honey-500)',
          }}
          aria-hidden="true"
        />
      </EdgeLabelRenderer>
      {reduced || !supportsOffsetPath ? (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px) rotate(${flip ? angle + 180 : angle}deg) scale(${flip ? -1 : 1}, 1)`,
              pointerEvents: 'none',
              color: broken ? 'var(--color-signal)' : 'var(--color-ink-800)',
            }}
            aria-hidden="true"
          >
            <div className="bee-bob">
              <BeeGlyph />
            </div>
          </div>
        </EdgeLabelRenderer>
      ) : (
        <EdgeLabelRenderer>
          <div
            className="motion-bee-ride"
            style={{
              position: 'absolute',
              left: 0,
              top: 0,
              offsetPath: `path("${path}")`,
              offsetRotate: 'auto',
              animationDelay: `-${rideDelay}s`,
              pointerEvents: 'none',
              color: broken ? 'var(--color-signal)' : 'var(--color-ink-800)',
            }}
            aria-hidden="true"
          >
            <div className="bee-bob">
              <BeeGlyph />
            </div>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

function BeeGlyph() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" aria-hidden="true" focusable="false">
      {/* Wings sit behind the body and beat a few degrees inside their frame. */}
      <g className="bee-wings">
        <g transform="rotate(-18 9.4 6.5)">
          <ellipse
            className="bee-wing"
            cx="9.4"
            cy="6.5"
            rx="3.3"
            ry="1.8"
            fill="var(--color-honey-100)"
            stroke="var(--color-ink-800)"
            strokeWidth="0.55"
            opacity="0.92"
          />
        </g>
        <g transform="rotate(-8 11 5.7)">
          <ellipse
            className="bee-wing bee-wing-far"
            cx="11"
            cy="5.7"
            rx="2.9"
            ry="1.5"
            fill="var(--color-honey-200)"
            stroke="var(--color-ink-800)"
            strokeWidth="0.45"
            opacity="0.7"
          />
        </g>
      </g>
      <path d="M4.4 10.1 2.6 10" stroke="var(--color-ink-800)" strokeWidth="1.1" strokeLinecap="round" />
      <ellipse
        cx="10"
        cy="10"
        rx="5.4"
        ry="3.5"
        fill="var(--color-honey-400)"
        stroke="var(--color-ink-800)"
        strokeWidth="0.7"
      />
      <path
        d="M7.6 7.2v5.6M10 6.9v6.2M12.4 7.2v5.6"
        stroke="var(--color-ink-800)"
        strokeWidth="1"
        strokeLinecap="round"
        opacity="0.7"
      />
      <circle cx="15.2" cy="10" r="1.9" fill="var(--color-ink-800)" />
      <path
        d="M15.9 8.7c.8-.8 1.7-1.1 2.5-.9M16 11.2c.8.6 1.7.8 2.4.6"
        fill="none"
        stroke="var(--color-ink-800)"
        strokeWidth="0.8"
        strokeLinecap="round"
      />
    </svg>
  );
}
