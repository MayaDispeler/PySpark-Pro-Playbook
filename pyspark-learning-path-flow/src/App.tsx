import React, { useEffect, useState } from "react";
import { motion } from "framer-motion";
import "./App.css";
import { ReactComponent as DatabaseIcon } from "./logos/database.svg";
import { ReactComponent as AnalyticsIcon } from "./logos/analytics.svg";
import { ReactComponent as ProcessIcon } from "./logos/process.svg";
import { ReactComponent as ChartIcon } from "./logos/chart.svg";
import { ReactComponent as SparkLogo } from "./logos/spark.svg";
import { ReactComponent as StreamIcon } from "./logos/stream.svg";
import { ReactComponent as TestIcon } from "./logos/test.svg";

const stages = [
  { label: "Data Generation", Icon: DatabaseIcon },
  { label: "Data Exploration", Icon: AnalyticsIcon },
  { label: "Transformations", Icon: ProcessIcon },
  { label: "Window Functions", Icon: ChartIcon },
  { label: "Machine Learning", Icon: SparkLogo },
  { label: "Streaming", Icon: StreamIcon },
  { label: "Testing", Icon: TestIcon },
];

// Smaller card size and more professional color
const nodeWidth = 90;
const nodeHeight = 90;
const spacing = 40;
const startX = 30;
const startY = 80;
const cardColor = "#f4f6fa";
const borderColor = "#1e293b";
const highlightColor = "#2563eb";

function getNodeX(index: number) {
  return startX + index * (nodeWidth + spacing);
}

function App() {
  const [activeIndex, setActiveIndex] = useState(-1);

  useEffect(() => {
    let i = 0;
    setActiveIndex(0);
    const interval = setInterval(() => {
      i++;
      if (i < stages.length) {
        setActiveIndex(i);
      } else {
        clearInterval(interval);
      }
    }, 700);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="App" style={{ background: "#f8fafc", minHeight: "100vh" }}>
      <h2 style={{ textAlign: "center", marginTop: 40, color: borderColor, letterSpacing: 1 }}>PySpark Learning Path</h2>
      <svg width={getNodeX(stages.length - 1) + nodeWidth + 40} height={260}>
        {/* Arrows */}
        {stages.slice(0, -1).map((_, i) => (
          <motion.line
            key={"arrow-" + i}
            x1={getNodeX(i) + nodeWidth}
            y1={startY + nodeHeight / 2}
            x2={getNodeX(i + 1)}
            y2={startY + nodeHeight / 2}
            stroke={i < activeIndex ? highlightColor : "#b0b8c9"}
            strokeWidth={5}
            strokeLinecap="round"
            initial={{ pathLength: 0 }}
            animate={{ pathLength: i < activeIndex ? 1 : 0 }}
            transition={{ delay: i * 0.4 + 0.5, duration: 0.5 }}
          />
        ))}
        {/* Nodes */}
        {stages.map((stage, i) => {
          const Icon = stage.Icon;
          return (
            <motion.g
              key={stage.label}
              initial={{ opacity: 0, scale: 0.7 }}
              animate={{
                opacity: 1,
                scale: activeIndex === i ? 1.12 : 1,
                filter: activeIndex === i ? `drop-shadow(0px 0px 12px ${highlightColor}55)` : "none",
              }}
              transition={{ delay: i * 0.4 }}
            >
              <rect
                x={getNodeX(i)}
                y={startY}
                width={nodeWidth}
                height={nodeHeight}
                rx={18}
                fill={cardColor}
                stroke={activeIndex === i ? highlightColor : borderColor}
                strokeWidth={3}
              />
              <g transform={`translate(${getNodeX(i) + 15}, ${startY + 15})`}>
                {/* Replace Icon below with real logo imports for production */}
                <Icon width={60} height={60} />
              </g>
              <text
                x={getNodeX(i) + nodeWidth / 2}
                y={startY + nodeHeight + 22}
                textAnchor="middle"
                fontSize={15}
                fill={borderColor}
                fontWeight="bold"
                style={{ letterSpacing: 0.5 }}
              >
                {stage.label}
              </text>
            </motion.g>
          );
        })}
      </svg>
      <p style={{ textAlign: "center", color: "#6b7280", marginTop: 18, fontSize: 15 }}>
        Each stage animates in sequence with real logos and highlight effect.<br/>
        <span style={{ fontSize: 13, color: "#94a3b8" }}>
          (For production, replace icons with official logos for each stage.)
        </span>
      </p>
    </div>
  );
}

export default App;
