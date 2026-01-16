// src/components/Chat.tsx - Tier3+4 UI (single-file replacement)
//
// Notes:
// - Preserves API_BASE and backend contract.
// - Keeps rowsStore approach (large rows kept outside React state).
// - Adds: right sidebar, message actions (edit/copy/delete/modal), chart modal, small UX improvements.
// - Ensure your project has framer-motion, lucide-react, recharts, axios installed (same deps as before).

import React, { useState, useRef, useEffect, useCallback } from 'react';
import html2canvas from "html2canvas";
import jsPDF from "jspdf";
<style>
  {`
  @keyframes fadeInUp {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .animate-fadeInUp {
    animation: fadeInUp 0.25s ease-out;
  }

  .ios-glow:focus {
    box-shadow:
      0 0 0 1px rgba(59,130,246,0.4),
      0 0 18px rgba(59,130,246,0.35),
      inset 0 0 0 1px rgba(255,255,255,0.25);
  }

  .dark .ios-glow:focus {
    box-shadow:
      0 0 0 1px rgba(96,165,250,0.45),
      0 0 22px rgba(96,165,250,0.4),
      inset 0 0 0 1px rgba(255,255,255,0.15);
  }

  .send-btn {
    transition:
      transform 0.15s ease,
      box-shadow 0.15s ease;
  }

  .send-btn:hover {
    transform: translateY(-1px);
    box-shadow: 0 6px 18px rgba(59,130,246,0.35);
  }

  .send-btn:active {
    transform: translateY(0) scale(0.96);
    box-shadow: 0 3px 10px rgba(59,130,246,0.25);
  }

  .input-dock {
    transition:
      transform 0.25s ease,
      box-shadow 0.25s ease;
  }

  .input-dock.is-active {
    transform: translateY(-4px);
    box-shadow:
      0 -8px 30px rgba(59,130,246,0.18),
      0 -2px 8px rgba(0,0,0,0.08);
  }

  .dark .input-dock.is-active {
    box-shadow:
      0 -10px 36px rgba(96,165,250,0.22),
      0 -3px 10px rgba(0,0,0,0.4);
  }
`}
</style>
import { motion } from 'framer-motion';
import {
  Send,
  Loader2,
  History,
  Moon,
  Sun,
  Table as TableIcon,
  Edit2,
  MoreHorizontal,
  Copy,
  Maximize2,
  X,
  Trash2
} from 'lucide-react';
import axios from 'axios';
import {
  LineChart as RechartsLineChart,
  Line,
  BarChart,
  AreaChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

const API_BASE = 'http://localhost:8000';
const MAX_MESSAGES = 200;


interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: string;
  data?: any;
}

/* -----------------------------------------
   Component: Chat
   - stable hooks ordering
   - rowsStore keeps large resultsets out of react state
   - sidebar and modal added
------------------------------------------*/
export default function Chat() {
  // core state
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string>('');
  const [darkMode, setDarkMode] = useState(false);

  // UI helpers
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarMenuOpen, setSidebarMenuOpen] = useState(false);
  const [chartModalOpen, setChartModalOpen] = useState(false);
  const [modalRows, setModalRows] = useState<any[] | null>(null);
  const [modalColumns, setModalColumns] = useState<string[] | null>(null);
  const [modalChartType, setModalChartType] = useState<string | undefined>(undefined);
  const [tableVisibleCount, setTableVisibleCount] = useState<Record<string, number>>({});

  // multi-chart expansion state (per message)
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Jump to Latest scroll helpers
  const [showJump, setShowJump] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  const handleScroll = useCallback(() => {
    if (!scrollRef.current) return;
    const el = scrollRef.current;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 150;
    setShowJump(!atBottom);
  }, []);

  // keep large rowsets outside react state to avoid re-rendering cost
  const rowsStore = useRef<Record<string, any[]>>({});

  useEffect(() => {
    setSessionId(crypto.randomUUID());
    const savedMode = localStorage.getItem('darkMode');
    if (savedMode === 'true') {
      setDarkMode(true);
      document.documentElement.classList.add('dark');
    }
  }, []);

  useEffect(() => {
    requestAnimationFrame(() => {
      messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    });
  }, [messages.length]);

  const toggleDarkMode = useCallback(() => {
    const html = document.documentElement;
    const isDark = html.classList.contains('dark');
    if (isDark) {
      html.classList.remove('dark');
      localStorage.setItem('darkMode', 'false');
      setDarkMode(false);
    } else {
      html.classList.add('dark');
      localStorage.setItem('darkMode', 'true');
      setDarkMode(true);
    }
  }, []);

  const pushMessage = useCallback((msg: Message) => {
    setMessages(prev => {
      const next = [...prev, msg];
      if (next.length > MAX_MESSAGES) return next.slice(next.length - MAX_MESSAGES);
      return next;
    });
  }, []);

  // Send handler — unchanged contract
  const handleSend = useCallback(async (userInput?: string, forcedTable?: string, forcedMetric?: string) => {
    const promptValue = (userInput ?? input).trim();
    if (!promptValue || loading) return;

    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: 'user',
      content: promptValue,
      timestamp: new Date().toISOString(),
    };

    pushMessage(userMessage);
    setInput('');
    setLoading(true);

    try {
      const payload: any = { prompt: promptValue, session_id: sessionId };
      if (forcedTable) payload.forced_table = forcedTable;
      if (forcedMetric) payload.forced_metric = forcedMetric;

      const response = await axios.post(`${API_BASE}/chat`, payload, { timeout: 120000 });
      const ctx = response.data.context ? response.data.context : response.data;

      // Clarification flow
      if ((ctx.needs_metric_clarification || ctx.needs_clarification) && Array.isArray(ctx.ui?.buttons)) {
        const type = ctx.needs_metric_clarification ? 'metric' : 'table';


        const systemMessage: Message = {
          id: crypto.randomUUID(),
          role: 'assistant',
          content: ctx.message || (
            type === 'metric'
              ? 'Multiple metrics match your query. Please select one:'
              : 'Multiple datasets match your query. Please select one:'
          ),
          timestamp: new Date().toISOString(),
          data: {
            ...ctx,
            _clarifyType: type,
            _originalPrompt: promptValue,
          },
        };

        pushMessage(systemMessage);
        setLoading(false);
        return;
      }


      let assistantData = {
        ...response.data,
        columns: ctx.columns || response.data.columns || [],
      };

      const msgId = crypto.randomUUID();

      // Keep rows directly on message.data (do NOT strip them)
      if (Array.isArray(response.data.rows)) {
        assistantData.rows = response.data.rows;
      }

      const assistantMessage: Message = {
        id: msgId,
        role: 'assistant',
        content:
          ctx.analysis_text ||
          ctx.analysis ||
          ctx.text ||
          ctx.message ||
          '',
        timestamp: new Date().toISOString(),
        data: assistantData,
      };

      pushMessage(assistantMessage);
    } catch (error) {
      console.error('Error:', error);
      pushMessage({
        id: crypto.randomUUID(),
        role: 'assistant',
        content: '❌ Something went wrong. Please try again.',
        timestamp: new Date().toISOString(),
      });
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  }, [input, loading, sessionId, pushMessage]);

  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  }, [handleSend]);

  const handleClear = useCallback(async () => {
    try {
      await axios.post(`${API_BASE}/clear_session/${sessionId}`);
      setMessages([]);
      rowsStore.current = {};
    } catch (error) {
      console.error('Failed to clear:', error);
    }

  }, [sessionId]);

  // Clarification handler
  const handleClarify = useCallback(
    (originalPrompt: string, clarifyType: 'table' | 'metric', selectedValue: string) => {
      if (loading) return;

      const enrichedPrompt =
        clarifyType === 'metric'
          ? `${originalPrompt}\n\n[User selected metric: ${selectedValue}]`
          : `${originalPrompt}\n\n[User selected table: ${selectedValue}]`;

      handleSend(enrichedPrompt, undefined, undefined);
    },
    [handleSend, loading]
  );

  // Helpers to get rows for a message (lazily)
  const getRowsForMessage = useCallback((m: Message) => {
    if (!m.data) return m.data?.rows || [];
    if (Array.isArray(m.data.rows) && m.data.rows.length > 0) return m.data.rows;
    if (m.data.rowsId) return rowsStore.current[m.data.rowsId] || [];
    return m.data.rows || [];
  }, []);


  const prettifyLabel = (column: string) =>
    column
      .replace(/_/g, " ")
      .replace(/\b(rs|mw|mwh|kwh|mu)\b/i, m => m.toUpperCase())
      .replace(/\s+/g, " ")
      .trim();



  /* ---------------------------
     Chart rendering (re-usable)
     - uses darkMode locally to pick axis/grid colors
  ----------------------------*/
  const renderChart = useCallback((rows: any[], columns: string[], chartType?: string) => {
    if (!rows || rows.length < 2 || !columns || columns.length === 0) {
      return (
        <div className="text-sm text-gray-500 dark:text-gray-400 italic p-4">
          No visualizable data available
        </div>
      );
    }
    // --- Normalize rows ---
    // Backend narrative charts may arrive as array-rows aligned with `columns`
    // Recharts expects object-shaped rows keyed by column names


    const normalizedRows = Array.isArray(rows[0])
      ? rows.map(r => {
        const obj: any = {};
        columns.forEach((c, i) => {
          obj[c] = r[i];
        });
        return obj;
      })
      : rows;

    const wrapLabel = (text: string, maxChars = 14) => {
      const words = text.split(" ");
      const lines: string[] = [];
      let current = "";

      for (const w of words) {
        if ((current + " " + w).trim().length > maxChars) {
          lines.push(current);
          current = w;
        } else {
          current = current ? current + " " + w : w;
        }
      }
      if (current) lines.push(current);
      return lines.slice(0, 2);
    };

    const axisColor = darkMode ? "#CBD5E1" : "#374151";
    const gridColor = darkMode ? "#475569" : "#E5E7EB";

    // Trust column order provided by multi-chart logic
    const xAxisCol = columns[0];

    // Metrics = remaining numeric columns (table-driven, based on order)
    const metricColumns = columns.slice(1).filter(col =>
      typeof normalizedRows[0]?.[col] === "number"
    );

    const yAxisLabel =
      metricColumns.length === 1
        ? prettifyLabel(metricColumns[0])
        : metricColumns.map(prettifyLabel).join(" / ");

    const normalizedType = (chartType || 'line').toLowerCase();

    const baseProps = {
      data: normalizedRows,
      margin: { top: 24, right: 48, left: 80, bottom: 110 }
    };

    const commonAxis = (
      <>
        <CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
        <XAxis
          dataKey={xAxisCol}
          stroke={axisColor}
          tick={{ fontSize: 11 }}
          tickMargin={8}
          height={80}
          label={{
            position: "insideBottom",
            offset: -8,
            content: ({ viewBox }: any) => {
              if (!viewBox) return null;

              const { x, y, width } = viewBox;
              const cx = x + width / 2;
              const cy = y + 50;

              const lines = wrapLabel(prettifyLabel(xAxisCol));

              return (
                <text
                  x={cx}
                  y={cy}
                  textAnchor="middle"
                  fill={axisColor}
                  fontSize={12}
                >
                  {lines.map((line, i) => (
                    <tspan key={i} x={cx} dy={i === 0 ? 0 : 14}>
                      {line}
                    </tspan>
                  ))}
                </text>
              );
            }
          }}
        />
        <YAxis
          yAxisId="left"
          stroke={axisColor}
          tick={{ fontSize: 11 }}
          width={60}
          label={{
            position: "insideLeft",
            content: ({ viewBox }: any) => {
              if (!viewBox) return null;

              const { x, y, height } = viewBox;
              const cx = x - 20;
              const cy = y + height / 2;

              const lines = wrapLabel(yAxisLabel);

              return (
                <g transform={`translate(${cx}, ${cy}) rotate(-90)`}>
                  <text
                    textAnchor="middle"
                    fill={axisColor}
                    fontSize={12}
                  >
                    {lines.map((line, i) => (
                      <tspan key={i} x={0} dy={i === 0 ? 0 : 14}>
                        {line}
                      </tspan>
                    ))}
                  </text>
                </g>
              );
            }
          }}
        />
        <YAxis
          yAxisId="right"
          orientation="right"
          stroke={axisColor}
          style={{ fontSize: '12px' }}
        />
        <Tooltip />
        {/* Legend removed from inside chart area */}
      </>
    );

    // --- Deterministic palette helper (per-chart, seeded by chart identity) ---
    const seededColorFor = (seed: string, index: number) => {
      // Simple deterministic hash (stable across renders)
      let h = 0;
      for (let i = 0; i < seed.length; i++) {
        h = (h << 5) - h + seed.charCodeAt(i);
        h |= 0;
      }

      // Base palette (kept local to avoid global reuse ordering)
      const PALETTE = [
        // 🔵 Blues / Cyans (limited, spaced)
        "#2563EB", // deep blue
        "#0284C7", // sky blue
        "#0891B2", // cyan

        // 🔴 Reds / Oranges
        "#DC2626", // strong red
        "#EA580C", // vivid orange
        "#F97316", // bright orange

        // 🟢 Greens
        "#16A34A", // dark green
        "#22C55E", // emerald
        "#65A30D", // olive green

        // 🟣 Purples / Violets
        "#7C3AED", // violet
        "#9333EA", // purple
        "#C026D3", // magenta

        // 🟡 Yellows / Golds (carefully chosen for dark bg)
        "#F59E0B", // amber
        "#EAB308", // yellow-gold
        "#CA8A04", // mustard

        // ⚫ Neutrals (last-resort / baseline comparisons)
        "#E5E7EB", // light gray (good on dark bg)
        "#94A3B8", // slate
      ];

      const offset = Math.abs(h) % PALETTE.length;
      return PALETTE[(offset + index) % PALETTE.length];
    };

    const renderSeries = () =>
      metricColumns.map((col, index) => {
        const isVolatility = col.toLowerCase().includes('volatility');

        const color = seededColorFor(
          // Seed on chart identity if available, otherwise column name
          `${xAxisCol}::${col}`,
          index
        );

        return (
          <Line
            key={col}
            dataKey={col}
            yAxisId={isVolatility ? "right" : "left"}
            stroke={color}
            strokeWidth={2}
            strokeDasharray={isVolatility ? "6 4" : undefined}
            dot={false}
            name={col.replaceAll("_", " ")}
          />
        );
      });

    let ChartComponent: any = RechartsLineChart;
    if (normalizedType === 'bar') ChartComponent = BarChart;
    if (normalizedType === 'area') ChartComponent = AreaChart;

    return (
      <div className="mt-4 w-full rounded-xl p-4 glass-sm">
        <ResponsiveContainer width="100%" height={380}>
          <ChartComponent {...baseProps}>
            {commonAxis}
            {renderSeries()}
          </ChartComponent>
        </ResponsiveContainer>
        {/* Adaptive legend (outside plot area to prevent chart collapse) */}
        <div
          className="mt-3 flex flex-wrap justify-center gap-x-4 gap-y-2"
          style={{
            maxHeight: metricColumns.length > 8 ? 72 : undefined,
            overflowY: metricColumns.length > 8 ? "auto" : "visible",
          }}
        >
          {metricColumns.map((col, idx) => {
            const color = seededColorFor(`${xAxisCol}::${col}`, idx);
            return (
              <div key={col} className="flex items-center gap-1 whitespace-nowrap">
                <span
                  style={{
                    width: 10,
                    height: 10,
                    backgroundColor: color,
                    borderRadius: 2,
                    display: "inline-block",
                  }}
                />
                <span
                  style={{
                    fontSize: metricColumns.length > 6 ? 10 : 12,
                    color: darkMode ? "#E5E7EB" : "#374151",
                  }}
                >
                  {prettifyLabel(col)}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    );
  }, [darkMode]);


  const renderDataTable = useCallback((rows: any[], columns: string[], tableKey: string) => {
    if (!rows || rows.length === 0) return null;
    const visibleCount = tableVisibleCount[tableKey] ?? 100;
    const displayRows = rows.slice(0, Math.min(visibleCount, rows.length));
    const displayCols = columns;

    return (
      <div
        className="mt-4 overflow-x-auto overflow-y-auto rounded-lg border border-gray-200 dark:border-gray-700"
        style={{ maxHeight: "420px" }}
      >
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 text-sm">
          <thead className="bg-gray-50 dark:bg-gray-800">
            <tr>
              {displayCols.map((col, idx) => (
                <th key={idx} className="px-4 py-3 text-left text-xs font-medium text-gray-700 dark:text-gray-300 uppercase tracking-wider">{col}</th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-800">
            {displayRows.map((row, rowIdx) => (
              <tr key={rowIdx} className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                {displayCols.map((col, colIdx) => (
                  <td key={colIdx} className="px-4 py-3 whitespace-nowrap text-gray-900 dark:text-gray-100">{row[col] !== null && row[col] !== undefined ? String(row[col]) : '-'}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {/* Load More button */}
        {visibleCount < rows.length && (
          <div className="p-3 text-center">
            <button
              onClick={() => setTableVisibleCount(prev => ({
                ...prev,
                [tableKey]: Math.min((prev[tableKey] ?? 100) + 100, rows.length)
              }))}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              Load More ({rows.length - visibleCount} remaining)
            </button>
          </div>
        )}
      </div>
    );
  }, [tableVisibleCount]);

  /* -------------------------
     Message actions (edit/copy/delete/open modal)
  -------------------------*/
  const openChartModal = useCallback((rows: any[], columns: string[] | undefined, chartType?: string) => {
    setModalRows(rows);
    setModalColumns(columns ?? (rows && rows[0] ? Object.keys(rows[0]) : []));
    setModalChartType(chartType);
    setChartModalOpen(true);
  }, []);

  const closeChartModal = useCallback(() => {
    setChartModalOpen(false);
    setModalRows(null);
    setModalColumns(null);
    setModalChartType(undefined);
  }, []);



  function exportToCSV(rows: any[], columns: string[], filename = "data.csv") {
    if (!rows?.length || !columns?.length) return;

    const header = columns.join(",");
    const csv = rows.map(row =>
      columns.map(col => {
        const v = row[col];
        if (v === null || v === undefined) return "";
        return `"${String(v).replace(/"/g, '""')}"`;
      }).join(",")
    );

    const blob = new Blob([header + "\n" + csv.join("\n")], {
      type: "text/csv;charset=utf-8;"
    });

    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
  }

  // PDF Export helper for assistant message
  const exportMessageToPDF = async (messageId: string) => {
    const el = document.getElementById(`assistant-message-${messageId}`);
    if (!el) return;

    const canvas = await html2canvas(el, {
      scale: 2,
      useCORS: true,
      backgroundColor: null,
    });

    const imgData = canvas.toDataURL("image/png");
    const pdf = new jsPDF("p", "mm", "a4");

    const pageWidth = pdf.internal.pageSize.getWidth();
    const pageHeight = pdf.internal.pageSize.getHeight();

    const imgWidth = pageWidth;
    const imgHeight = (canvas.height * imgWidth) / canvas.width;

    let y = 0;
    let remainingHeight = imgHeight;

    while (remainingHeight > 0) {
      pdf.addImage(imgData, "PNG", 0, y, imgWidth, imgHeight);
      remainingHeight -= pageHeight;
      if (remainingHeight > 0) {
        pdf.addPage();
        y -= pageHeight;
      }
    }

    pdf.save(`energy-report-${messageId}.pdf`);
  };


  /* -------------------------
     Render - layout with Sidebar
  -------------------------*/
  return (
    <div className="flex h-screen bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800">
      {/* Main column */}
      <div className="flex-1 flex flex-col">
        <header className="glass-md px-6 py-4 flex items-center justify-between shadow-[0_1px_0_rgba(255,255,255,0.08)]">
          <div className="flex items-center gap-3">
            <motion.div className="w-10 h-10 rounded-full bg-gradient-to-r from-blue-500 to-purple-600 flex items-center justify-center text-white font-bold shadow-lg">AI</motion.div>
            <div>
              <h1 className="text-lg font-semibold text-gray-900 dark:text-white">Energy Assistant</h1>
              <p className="text-xs text-gray-500 dark:text-gray-400">Gemini-3-flash</p>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button onClick={() => setSidebarMenuOpen(o => !o)} className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg" title="Toggle sidebar">
              <MoreHorizontal className="w-5 h-5 text-gray-600 dark:text-gray-300" />
            </button>
            {sidebarMenuOpen && (
              <div className="fixed inset-0 z-50 flex items-start justify-end p-6 bg-black/40 backdrop-blur-sm" onClick={() => setSidebarMenuOpen(false)}>
                <div
                  className="w-64 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-2xl shadow-2xl p-3 animate-fadeInUp"
                  onClick={(e) => e.stopPropagation()}
                >
                  <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100 mb-3">Quick Actions</h4>

                  <button
                    onClick={() => { setSidebarOpen(s => !s); setSidebarMenuOpen(false); }}
                    className="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 text-sm text-gray-800 dark:text-gray-100"
                  >
                    Toggle Sidebar
                  </button>

                  <button
                    onClick={() => { toggleDarkMode(); setSidebarMenuOpen(false); }}
                    className="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 text-sm text-gray-800 dark:text-gray-100"
                  >
                    Toggle Theme
                  </button>

                  <button
                    onClick={() => { handleClear(); setSidebarMenuOpen(false); }}
                    className="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 text-sm text-gray-800 dark:text-gray-100 mt-1"
                  >
                    Clear Conversation
                  </button>
                </div>
              </div>
            )}
            <button onClick={toggleDarkMode} className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg" title={darkMode ? 'Light mode' : 'Dark mode'}>
              {darkMode ? <Sun className="w-5 h-5 text-yellow-500" /> : <Moon className="w-5 h-5 text-gray-600" />}
            </button>
            <button
              onClick={handleClear}
              className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg"
              title="Clear history"
            >
              <Trash2 className="w-5 h-5 text-gray-600 dark:text-gray-400" />
            </button>
          </div>
        </header>

        <main
          className="flex-1 overflow-y-auto px-4 py-6"
          ref={scrollRef}
          onScroll={handleScroll}
        >
          {messages.length > 0 && showJump && (
            <motion.button
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 20 }}
              onClick={() => { messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }}
              className="fixed bottom-28 right-8 z-40 px-4 py-2 rounded-full bg-blue-600/80 backdrop-blur-lg text-white shadow-xl hover:bg-blue-700 transition-all dark:bg-blue-500/80 dark:hover:bg-blue-600"
            >
              Jump to Latest
            </motion.button>
          )}
          <div className="max-w-6xl mx-auto space-y-6">
            {messages.length === 0 && (
              <div className="text-center py-12">
                <div className="w-20 h-20 mx-auto mb-4 rounded-full bg-gradient-to-r from-blue-500 to-purple-600 flex items-center justify-center shadow-xl"><History className="w-10 h-10 text-white" /></div>
                <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">Start a conversation</h2>
                <p className="text-gray-500 dark:text-gray-400 mb-6">Ask me about energy data</p>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-4xl mx-auto mt-6">
                  {[
                    {
                      title: 'Compare DAM Volume vs Price',
                      subtitle: 'Side-by-side metric trends',
                      prompt: 'Compare DAM cleared volume and price trend'
                    },
                    {
                      title: 'Block-wise Demand',
                      subtitle: '15-min interval analysis',
                      prompt: 'Show block wise demand'
                    },
                    {
                      title: 'FSV & MCP Trend for DAM',
                      subtitle: 'Market scheduling comparison',
                      prompt: 'Show FSV and MCP trend for DAM'
                    }
                  ].map(card => (
                    <button
                      key={card.title}
                      onClick={() => setInput(card.prompt)}
                      className="glass-sm rounded-2xl p-4 text-left hover:scale-[1.02] transition-all duration-300 shadow-md"
                    >
                      <h3 className="font-semibold text-gray-900 dark:text-white">
                        {card.title}
                      </h3>
                      <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                        {card.subtitle}
                      </p>
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Messages */}
            {messages.map((message, index) => {
              const prev = messages[index - 1];
              const next = messages[index + 1];

              const isFirstInGroup = !prev || prev.role !== message.role;
              const isLastInGroup = !next || next.role !== message.role;

              const isUser = message.role === 'user';
              const isAssistant = message.role === 'assistant';

              const rows = getRowsForMessage(message);
              const cols =
                message.data?.columns ||
                (rows.length ? Object.keys(rows[0]) : []);

              /* -----------------------------------------
                 UNIVERSAL MULTI-CHART INFERENCE (RELAXED)
                 - triggers when cols.length >= 2
                 - no monotonic enforcement
                 - works for all segment-style datasets
              ------------------------------------------*/
              const hasNarrativeBlocks = Array.isArray(message.data?.narrative_blocks);
              let autoCharts: any[] | null = null;

              if (
                !hasNarrativeBlocks &&
                !Array.isArray(message.data?.charts) &&
                rows.length > 1 &&
                cols.length >= 2
              ) {
                // 1️⃣ grouping column: first low-cardinality string column
                const groupCol =
                  cols.find((c: string) => {
                    const v = rows[0]?.[c];
                    if (typeof v !== "string") return false;
                    const uniq = new Set(rows.map((r: any) => r[c]));
                    return uniq.size > 1 && uniq.size <= 20;
                  }) || null;

                // 2️⃣ numeric columns (accept numeric-like strings)
                const isNumericLike = (v: any) =>
                  typeof v === "number" ||
                  (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v)));

                const numericCols = cols.filter((c: string) =>
                  isNumericLike(rows[0]?.[c])
                );

                // 3️⃣ x-axis heuristic (name-based, fallback to first numeric)
                const xCol =
                  numericCols.find((c: string) =>
                    /time|block|date|hour|interval|seq|index/i.test(c)
                  ) || numericCols[0];

                // 4️⃣ metrics = remaining numeric columns, exclude groupCol
                const metricCols = numericCols.filter(
                  (c: string) => c !== xCol && c !== groupCol
                );

                if (groupCol && xCol && metricCols.length > 0) {
                  autoCharts = Array.from(new Set(rows.map((r: any) => r[groupCol]))).map(
                    (groupValue: any) => {
                      const groupRows = rows
                        .filter((r: any) => r[groupCol] === groupValue)
                        .sort((a: any, b: any) => {
                          const av = Number(a[xCol]);
                          const bv = Number(b[xCol]);
                          if (isNaN(av) || isNaN(bv)) return 0;
                          return av - bv;
                        });

                      return {
                        title: `${groupCol}: ${groupValue}`,
                        chart_type: message.data?.chart_type || "line",
                        rows: groupRows,
                        columns: [xCol, ...metricCols],
                      };
                    }
                  );
                }
              }

              // Base charts: ONLY from autoCharts (never narrative charts)
              const baseCharts = !hasNarrativeBlocks ? autoCharts : null;

              const bubbleBase =
                isUser
                  ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white ml-auto'
                  : 'glass-sm text-gray-900 dark:text-white';

              const bubbleShape = `
                ${isFirstInGroup ? 'mt-4' : 'mt-1'}
                ${isLastInGroup ? 'mb-4' : 'mb-1'}
                ${isFirstInGroup && isAssistant ? 'rounded-3xl rounded-tl-sm' : ''}
                ${isFirstInGroup && isUser ? 'rounded-3xl rounded-tr-sm' : ''}
                ${!isFirstInGroup ? 'rounded-2xl' : ''}
                ${isLastInGroup ? '' : 'rounded-b-xl'}
              `;

              return (
                <motion.div
                  key={message.id}
                  initial={{ opacity: 0, y: 8, scale: 0.98 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  transition={{ duration: 0.28, ease: 'easeOut' }}
                  className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}
                >
                  {isAssistant ? (
                    <div
                      id={`assistant-message-${message.id}`}
                      className={`relative inline-block min-w-[50%] w-fit max-w-full px-7 py-5
shadow-lg transition-all duration-300 ${bubbleBase} ${bubbleShape}`}
                    >
                      {/* Message text / Narrative blocks */}
                      {isAssistant && Array.isArray(message.data?.narrative_blocks)
                        ? message.data.narrative_blocks.map((block: any, idx: number) => {
                          // --- Inline metric filter for this block ---
                          const blockText = (block.text || "").toLowerCase();

                          // Paragraph-wise deterministic chart slicing for this block only
                          const blockCharts =
                            (message.data?.charts || [])
                              .filter((c: any) => c.paragraph_id === block.id)
                              .map((chart: any) => {
                                if (!chart?.columns || !chart?.rows) return chart;
                                const xCol = chart.columns[0];
                                const metricCols = chart.columns.slice(1);
                                // Semantic, deterministic metric selection per paragraph
                                const text = blockText;
                                const semanticMatchers: Array<{ rx: RegExp; cols: RegExp }> = [
                                  { rx: /executive|summary|overall|system|total/i, cols: /total/i },
                                  { rx: /thermal/i, cols: /thermal/i },
                                  { rx: /renewable|solar|wind/i, cols: /renewable|solar|wind/i },
                                  { rx: /hydro/i, cols: /hydro/i },
                                  { rx: /nuclear/i, cols: /nuclear/i },
                                  { rx: /volatility|range|fluctuat/i, cols: /volatility|range/i },
                                ];
                                let selectedMetrics: string[] = [];
                                // Match paragraph intent → metric columns
                                for (const rule of semanticMatchers) {
                                  if (rule.rx.test(text)) {
                                    const matched = metricCols.filter((c: string) =>
                                      rule.cols.test(c.toLowerCase())
                                    );
                                    if (matched.length >= 1) {
                                      selectedMetrics = matched;
                                      break;
                                    }
                                  }
                                }
                                // Safety fallback: never show identical charts everywhere
                                if (selectedMetrics.length === 0) {
                                  selectedMetrics = metricCols.slice(0, Math.min(2, metricCols.length));
                                }
                                // Safety fallback
                                if (selectedMetrics.length < 1) {
                                  selectedMetrics = metricCols;
                                }
                                const nextColumns = [xCol, ...selectedMetrics];
                                const colIdx = nextColumns.map((c: string) =>
                                  chart.columns.indexOf(c)
                                );
                                const nextRows = Array.isArray(chart.rows?.[0])
                                  ? chart.rows.map((r: any[]) => colIdx.map(i => r[i]))
                                  : chart.rows;
                                return {
                                  ...chart,
                                  id: `${chart.id}__${block.id}`,
                                  columns: nextColumns,
                                  rows: nextRows,
                                };
                              });

                          return (
                            <div key={block.id || idx} className="mb-6">
                              {/* Narrative text */}
                              <p className="whitespace-pre-wrap leading-relaxed mb-3">
                                {block.text}
                              </p>
                              {/* Supporting charts: always render immediately after paragraph, before evidence */}
                              {blockCharts.length > 0 && (
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-3">
                                  {blockCharts.map((chart: any, cidx: number) => {
                                    const safeColumns =
                                      chart.columns && chart.columns.length >= 3
                                        ? chart.columns
                                        : chart.columns;
                                    const safeRows = chart.rows;
                                    return (
                                      <div key={cidx} className="glass rounded-xl p-4">
                                        <div className="text-sm font-semibold mb-2">
                                          {chart.title || "Supporting Chart"}
                                        </div>
                                        {renderChart(safeRows, safeColumns, chart.chart_type)}
                                      </div>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                          );
                        })
                        : (
                          <p className="whitespace-pre-wrap leading-relaxed mb-2">
                            {(() => {
                              const text =
                                String(message.content ?? '')
                                  .replace(/\*{1,}[^*]*\*{1,}/g, '')
                                  .replace(/[^\S\r\n]{2,}/g, ' ')
                                  .replace(/\n{3,}/g, '\n\n')
                                  .trim();
                              return text;
                            })()}
                          </p>
                        )
                      }
                      {/* Evidence divider and header */}
                      {isAssistant && rows.length > 0 && (
                        <div className="mt-3 mb-2 flex items-center gap-2 text-xs font-semibold text-blue-600 dark:text-blue-400">
                          <span className="px-2 py-0.5 rounded-full bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700">
                            Evidence
                          </span>
                          <span className="text-gray-500 dark:text-gray-400">
                            The analysis above is supported by the data below
                          </span>
                        </div>
                      )}

                      {/* Base chart (overall data trend) — unchanged legacy behavior */}
                      {isAssistant && baseCharts && baseCharts.length > 0 && (
                        <div className="mt-4">
                          <div className="text-sm font-semibold mb-2">
                            Overall Data Trend
                          </div>

                          {/* Render ONLY the first base chart for cleanliness */}
                          {renderChart(
                            baseCharts[0].rows,
                            baseCharts[0].columns,
                            baseCharts[0].chart_type
                          )}
                        </div>
                      )}

                      {/* Floating clarification actions (minimal, user-message only) */}
                      {isAssistant &&
                        Array.isArray(message.data?.ui?.buttons) && (
                          <div
                            className="
                            absolute right-0 -bottom-10
                            flex flex-wrap gap-2
                            bg-white/90 dark:bg-gray-800/90
                            backdrop-blur-md
                            border border-gray-200 dark:border-gray-700
                            rounded-xl px-2 py-1.5
                            shadow-lg
                            animate-fadeInUp
                          "
                          >
                            {message.data.ui.buttons.map((btn: any, idx: number) => (
                              <button
                                key={btn.metric || btn.table || idx}
                                onClick={() =>
                                  handleClarify(
                                    message.data._originalPrompt,
                                    message.data._clarifyType,
                                    btn.metric ?? btn.table
                                  )
                                }
                                disabled={loading}
                                className="
                                px-3 py-1.5
                                text-xs font-medium
                                rounded-full
                                bg-blue-600 text-white
                                hover:bg-blue-700
                                disabled:opacity-50
                                transition
                                whitespace-nowrap
                              "
                              >
                                {btn.label || btn.metric || btn.table}
                              </button>
                            ))}
                          </div>
                        )}

                      {/* User prompt actions */}
                      {isUser && (
                        <div className="flex gap-2 items-center text-xs mt-2 justify-end">
                          <button
                            onClick={() => {
                              setInput(message.content);
                              inputRef.current?.focus();
                            }}
                            title="Edit prompt"
                            className="px-2 py-1 rounded hover:bg-white/20"
                          >
                            <Edit2 className="w-4 h-4 text-white" />
                          </button>
                          <button
                            onClick={() => navigator.clipboard.writeText(message.content)}
                            title="Copy prompt"
                            className="px-2 py-1 rounded hover:bg-white/20"
                          >
                            <Copy className="w-4 h-4 text-white" />
                          </button>
                        </div>
                      )}

                      {/* Assistant table */}
                      {isAssistant && rows.length > 0 && (
                        <div className="mt-4">
                          <div className="flex items-center justify-between mb-2 text-sm">
                            <div className="flex items-center gap-2">
                              <TableIcon className="w-4 h-4 text-blue-500" />
                              <span className="font-semibold">
                                Data Table ({rows.length} rows)
                              </span>
                            </div>

                            <div className="flex items-center gap-2">
                              <button
                                onClick={() => exportToCSV(rows, cols)}
                                className="px-2 py-1 rounded-md text-xs font-medium
                                         bg-white/80 dark:bg-gray-700/80
                                         text-gray-900 dark:text-gray-100
                                         border border-gray-300 dark:border-gray-600
                                         hover:bg-white dark:hover:bg-gray-600
                                         transition"
                              >
                                Export CSV
                              </button>

                              <button
                                onClick={() => openChartModal(rows, cols)}
                                className="p-1 rounded-md
                                         hover:bg-gray-100 dark:hover:bg-gray-700
                                         transition"
                                title="View fullscreen"
                              >
                                <Maximize2 className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                          {renderDataTable(rows, cols, message.id)}
                        </div>
                      )}



                      {/* Download as PDF button */}
                      <div className="mt-4 flex justify-end">
                        <button
                          onClick={() => exportMessageToPDF(message.id)}
                          className="px-3 py-1.5 text-xs font-medium rounded-lg
                                   bg-white/80 dark:bg-gray-700/80
                                   text-gray-900 dark:text-gray-100
                                   border border-gray-300 dark:border-gray-600
                                   hover:bg-white dark:hover:bg-gray-600
                                   transition"
                        >
                          Download as PDF
                        </button>
                      </div>
                      {isLastInGroup && (
                        <p className="text-xs opacity-60 mt-3">
                          {new Date(message.timestamp).toLocaleTimeString()}
                        </p>
                      )}
                    </div>
                  ) : (
                    <div
                      className={`relative inline-block ${isUser ? 'max-w-fit' : 'min-w-[50%] w-fit max-w-full'
                        } ${isAssistant ? 'px-7 py-5' : 'px-5 py-3'}
                    shadow-lg transition-all duration-300 ${bubbleBase} ${bubbleShape}`}
                    >
                      {/* Message text / Narrative blocks */}
                      {isAssistant && Array.isArray(message.data?.narrative_blocks)
                        ? message.data.narrative_blocks.map((block: any, idx: number) => {
                          // --- Inline metric filter for this block ---
                          const blockText = (block.text || "").toLowerCase();

                          // Paragraph-wise deterministic chart slicing for this block only
                          const blockCharts =
                            (message.data?.charts || [])
                              .filter((c: any) => c.paragraph_id === block.id)
                              .map((chart: any) => {
                                if (!chart?.columns || !chart?.rows) return chart;
                                const xCol = chart.columns[0];
                                const metricCols = chart.columns.slice(1);
                                // Semantic, deterministic metric selection per paragraph
                                const text = blockText;
                                const semanticMatchers: Array<{ rx: RegExp; cols: RegExp }> = [
                                  { rx: /executive|summary|overall|system|total/i, cols: /total/i },
                                  { rx: /thermal/i, cols: /thermal/i },
                                  { rx: /renewable|solar|wind/i, cols: /renewable|solar|wind/i },
                                  { rx: /hydro/i, cols: /hydro/i },
                                  { rx: /nuclear/i, cols: /nuclear/i },
                                  { rx: /volatility|range|fluctuat/i, cols: /volatility|range/i },
                                ];
                                let selectedMetrics: string[] = [];
                                // Match paragraph intent → metric columns
                                for (const rule of semanticMatchers) {
                                  if (rule.rx.test(text)) {
                                    const matched = metricCols.filter((c: string) =>
                                      rule.cols.test(c.toLowerCase())
                                    );
                                    if (matched.length >= 1) {
                                      selectedMetrics = matched;
                                      break;
                                    }
                                  }
                                }
                                // Safety fallback: never show identical charts everywhere
                                if (selectedMetrics.length === 0) {
                                  selectedMetrics = metricCols.slice(0, Math.min(2, metricCols.length));
                                }
                                // Safety fallback
                                if (selectedMetrics.length < 1) {
                                  selectedMetrics = metricCols;
                                }
                                const nextColumns = [xCol, ...selectedMetrics];
                                const colIdx = nextColumns.map((c: string) =>
                                  chart.columns.indexOf(c)
                                );
                                const nextRows = Array.isArray(chart.rows?.[0])
                                  ? chart.rows.map((r: any[]) => colIdx.map(i => r[i]))
                                  : chart.rows;
                                return {
                                  ...chart,
                                  id: `${chart.id}__${block.id}`,
                                  columns: nextColumns,
                                  rows: nextRows,
                                };
                              });

                          return (
                            <div key={block.id || idx} className="mb-6">
                              {/* Narrative text */}
                              <p className="whitespace-pre-wrap leading-relaxed mb-3">
                                {block.text}
                              </p>
                              {/* Supporting charts: always render immediately after paragraph, before evidence */}
                              {blockCharts.length > 0 && (
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-3">
                                  {blockCharts.map((chart: any, cidx: number) => {
                                    const safeColumns =
                                      chart.columns && chart.columns.length >= 3
                                        ? chart.columns
                                        : chart.columns;
                                    const safeRows = chart.rows;
                                    return (
                                      <div key={cidx} className="glass rounded-xl p-4">
                                        <div className="text-sm font-semibold mb-2">
                                          {chart.title || "Supporting Chart"}
                                        </div>
                                        {renderChart(safeRows, safeColumns, chart.chart_type)}
                                      </div>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                          );
                        })
                        : (
                          <p className="whitespace-pre-wrap leading-relaxed mb-2">
                            {(() => {
                              const text =
                                String(message.content ?? '')
                                  .replace(/\*{1,}[^*]*\*{1,}/g, '')
                                  .replace(/[^\S\r\n]{2,}/g, ' ')
                                  .replace(/\n{3,}/g, '\n\n')
                                  .trim();
                              return text;
                            })()}
                          </p>
                        )
                      }
                      {/* Evidence divider and header */}
                      {isAssistant && rows.length > 0 && (
                        <div className="mt-3 mb-2 flex items-center gap-2 text-xs font-semibold text-blue-600 dark:text-blue-400">
                          <span className="px-2 py-0.5 rounded-full bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700">
                            Evidence
                          </span>
                          <span className="text-gray-500 dark:text-gray-400">
                            The analysis above is supported by the data below
                          </span>
                        </div>
                      )}

                      {/* Floating clarification actions (minimal, user-message only) */}
                      {isAssistant &&
                        Array.isArray(message.data?.ui?.buttons) && (
                          <div
                            className="
                            absolute right-0 -bottom-10
                            flex flex-wrap gap-2
                            bg-white/90 dark:bg-gray-800/90
                            backdrop-blur-md
                            border border-gray-200 dark:border-gray-700
                            rounded-xl px-2 py-1.5
                            shadow-lg
                            animate-fadeInUp
                          "
                          >
                            {message.data.ui.buttons.map((btn: any, idx: number) => (
                              <button
                                key={btn.metric || btn.table || idx}
                                onClick={() =>
                                  handleClarify(
                                    message.data._originalPrompt,
                                    message.data._clarifyType,
                                    btn.metric ?? btn.table
                                  )
                                }
                                disabled={loading}
                                className="
                                px-3 py-1.5
                                text-xs font-medium
                                rounded-full
                                bg-blue-600 text-white
                                hover:bg-blue-700
                                disabled:opacity-50
                                transition
                                whitespace-nowrap
                              "
                              >
                                {btn.label || btn.metric || btn.table}
                              </button>
                            ))}
                          </div>
                        )}

                      {/* User prompt actions */}
                      {isUser && (
                        <div className="flex gap-2 items-center text-xs mt-2 justify-end">
                          <button
                            onClick={() => {
                              setInput(message.content);
                              inputRef.current?.focus();
                            }}
                            title="Edit prompt"
                            className="px-2 py-1 rounded hover:bg-white/20"
                          >
                            <Edit2 className="w-4 h-4 text-white" />
                          </button>
                          <button
                            onClick={() => navigator.clipboard.writeText(message.content)}
                            title="Copy prompt"
                            className="px-2 py-1 rounded hover:bg-white/20"
                          >
                            <Copy className="w-4 h-4 text-white" />
                          </button>
                        </div>
                      )}

                      {/* Assistant table */}
                      {isAssistant && rows.length > 0 && (
                        <div className="mt-4">
                          <div className="flex items-center justify-between mb-2 text-sm">
                            <div className="flex items-center gap-2">
                              <TableIcon className="w-4 h-4 text-blue-500" />
                              <span className="font-semibold">
                                Data Table ({rows.length} rows)
                              </span>
                            </div>

                            <div className="flex items-center gap-2">
                              <button
                                onClick={() => exportToCSV(rows, cols)}
                                className="px-2 py-1 rounded-md text-xs font-medium
                                         bg-white/80 dark:bg-gray-700/80
                                         text-gray-900 dark:text-gray-100
                                         border border-gray-300 dark:border-gray-600
                                         hover:bg-white dark:hover:bg-gray-600
                                         transition"
                              >
                                Export CSV
                              </button>

                              <button
                                onClick={() => openChartModal(rows, cols)}
                                className="p-1 rounded-md
                                         hover:bg-gray-100 dark:hover:bg-gray-700
                                         transition"
                                title="View fullscreen"
                              >
                                <Maximize2 className="w-4 h-4" />
                              </button>
                            </div>
                          </div>
                          {renderDataTable(rows, cols, message.id)}
                        </div>
                      )}



                      {isLastInGroup && (
                        <p className="text-xs opacity-60 mt-3">
                          {new Date(message.timestamp).toLocaleTimeString()}
                        </p>
                      )}
                    </div>
                  )}
                </motion.div>
              );
            })}

            {loading && (
              <div className="flex justify-start">
                <div className="max-w-3xl px-4 py-2 rounded-2xl bg-white/70 dark:bg-gray-800/70 border border-gray-200 dark:border-gray-700 backdrop-blur-sm shadow-sm flex items-center gap-2 transition-all duration-300">
                  <motion.div
                    className="flex gap-1"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ duration: 0.2 }}
                  >
                    <span className="w-2 h-2 bg-blue-500 rounded-full animate-bounce [animation-delay:-0.2s]"></span>
                    <span className="w-2 h-2 bg-blue-500 rounded-full animate-bounce [animation-delay:-0.1s]"></span>
                    <span className="w-2 h-2 bg-blue-500 rounded-full animate-bounce"></span>
                  </motion.div>
                  <span className="text-xs text-gray-700 dark:text-gray-300">Thinking…</span>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        </main>

        {/* Input area */}
        <div
          className={`input-dock glass-md px-4 py-4 sticky bottom-0 z-30 shadow-[0_-1px_0_rgba(255,255,255,0.08)] ${input.trim().length > 0 ? 'is-active' : ''
            }`}
        >
          <div className="max-w-4xl mx-auto flex items-end gap-3">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyPress={handleKeyPress}
              onInput={(e) => {
                const ta = e.currentTarget;
                ta.style.height = "auto";
                ta.style.height = ta.scrollHeight + "px";
              }}
              placeholder="Ask about energy data..."
              className="ios-glow w-full px-4 py-3 rounded-xl border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:border-blue-500 dark:focus:border-blue-400 focus:ring-0 outline-none transition-all duration-300 resize-none shadow-sm overflow-hidden"
              rows={1}
              disabled={loading}
            />
            <button
              onClick={() => handleSend()}
              disabled={!input.trim() || loading}
              className="send-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 shrink-0"
            >
              {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Send className="w-5 h-5" />}
            </button>
          </div>
        </div>
      </div >

      {/* Right Sidebar (Tier-3/4) */}
      < aside className={`glass-lg w-80 transition-transform duration-300 dark:border-gray-700/30 shadow-2xl transition-transform duration-300 ${sidebarOpen ? 'translate-x-0' : 'translate-x-full'} fixed right-0 top-0 h-full z-50`
      }>
        <div className="px-5 py-4 flex items-center justify-between border-b border-gray-100 dark:border-gray-800 bg-white/30 dark:bg-gray-800/30 backdrop-blur-xl">
          <h3 className="font-semibold text-gray-900 dark:text-gray-100">Session Tools</h3>
          <button onClick={() => setSidebarOpen(false)} className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded">
            <X className="w-4 h-4 text-gray-700 dark:text-gray-200" />
          </button>
        </div>

        <div className="px-4 py-3 space-y-3">
          <button
            onClick={() => { navigator.clipboard?.writeText(sessionId); }}
            className="w-full px-4 py-3 rounded-xl bg-white/20 dark:bg-gray-700/20 border border-white/20 dark:border-gray-600/20 backdrop-blur-xl text-sm text-left text-gray-900 dark:text-gray-100 hover:bg-white/30 dark:hover:bg-gray-700/30 transition-all duration-300 shadow-lg"
          >
            Copy session id
          </button>
          <button
            onClick={() => setMessages([])}
            className="w-full px-4 py-3 rounded-xl bg-white/20 dark:bg-gray-700/20 border border-white/20 dark:border-gray-600/20 backdrop-blur-xl text-sm text-left text-gray-900 dark:text-gray-100 hover:bg-white/30 dark:hover:bg-gray-700/30 transition-all duration-300 shadow-lg"
          >
            Clear conversation
          </button>
          <div className="pt-2 border-t border-gray-100 dark:border-gray-800">
            <h4 className="text-xs text-gray-600 dark:text-gray-300 mb-2">Recent prompts</h4>
            <div className="flex flex-col gap-2 max-h-48 overflow-y-auto">
              {messages.filter(m => m.role === 'user').slice(-12).reverse().map((m) => (
                <button
                  key={m.id}
                  onClick={() => setInput(m.content)}
                  className="text-left px-3 py-2 rounded-lg hover:bg-white/30 dark:hover:bg-gray-700/30 text-sm text-gray-900 dark:text-gray-100 transition cursor-pointer"
                >
                  {m.content.slice(0, 60)}
                </button>
              ))}
              {messages.filter(m => m.role === 'user').length === 0 && <div className="text-xs text-gray-400">No prompts yet</div>}
            </div>
          </div>
        </div>
      </aside >

      {/* Chart Modal */}
      {
        chartModalOpen && modalRows && modalColumns && (
          <motion.div
            className="fixed inset-0 z-50 flex items-center justify-center"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          >
            <motion.div
              className="absolute inset-0 bg-black/50"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              onClick={closeChartModal}
            />
            <motion.div
              className="glass-lg relative w-11/12 max-w-6xl rounded-2xl overflow-hidden"
              initial={{ opacity: 0, scale: 0.9, y: 40 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.9, y: 40 }}
              transition={{ duration: 0.35, ease: [0.16, 1, 0.3, 1] }}
            >
              <div className="px-4 py-3 flex items-center justify-between
                            border-b border-white/20 dark:border-gray-700/30
                            bg-white/40 dark:bg-gray-800/40
                            backdrop-blur-xl
                            sticky top-0 z-30">
                <h3 className="font-semibold text-gray-900 dark:text-gray-100">
                  Chart viewer
                </h3>

                <div className="flex items-center gap-2">
                  <button
                    onClick={() => exportToCSV(modalRows, modalColumns)}
                    className="px-3 py-1.5 rounded-lg text-sm font-medium bg-white dark:bg-gray-700
text-gray-900 dark:text-gray-100
hover:bg-gray-100 dark:hover:bg-gray-600
border border-gray-300 dark:border-gray-600 hover:bg-white/40 dark:hover:bg-gray-600/40 border border-white/20 dark:border-gray-600/30"
                  >
                    Export CSV
                  </button>

                  <button
                    onClick={closeChartModal}
                    aria-label="Close chart modal"
                    className="p-2 rounded-lg
                             bg-white/30 dark:bg-gray-700/30
                             hover:bg-white/40 dark:hover:bg-gray-600/40
                             border border-white/20 dark:border-gray-600/30
                             backdrop-blur-md transition"
                  >
                    <X className="w-5 h-5 text-gray-900 dark:text-gray-100" />
                  </button>
                </div>
              </div>
              <div className="p-4 max-h-[85vh] overflow-y-auto">
                <div className="sticky top-0 bg-white dark:bg-gray-900 z-20 pb-4">
                  {renderChart(modalRows, modalColumns, modalChartType)}
                </div>
                <div className="mt-4">
                  {renderDataTable(modalRows, modalColumns, 'modal')}
                </div>
              </div>
            </motion.div>
          </motion.div>
        )
      }
    </div >
  );
}