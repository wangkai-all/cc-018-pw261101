import { useEffect, useMemo, useRef, useState } from "react";

const SCENARIOS = {
  append: {
    path: "/api/demo/append-during-compaction",
    title: "合并中追加：冲突检测与重规划",
    description: "合并任务已基于 vN 规划并校验输出，此时追加写入产生 vN+1；发布必须冲突并重新规划。",
  },
  dual: {
    path: "/api/demo/dual-compaction",
    title: "双轮并发合并：只有一个能发布",
    description: "两轮任务读取同一个 base manifest；先发布者胜出，后发布者基于新 current 重新规划。",
  },
  failure: {
    path: "/api/demo/failure-before-publish",
    title: "发布前失败：版本与数据都不变",
    description: "输出已写入临时区并通过校验，但在发布前注入失败；临时文件不进入 manifest。",
  },
  repeat: {
    path: "/api/demo/repeat-compaction",
    title: "重复发起合并：不会重复计算",
    description: "两个任务同时处理同一批小文件，未发布的那一轮不能覆盖已发布版本。",
  },
  pinGc: {
    path: "/api/demo/pin-and-gc",
    title: "固定旧版 → 清理 → 解除固定回收",
    description: "先固定旧 manifest，追加多版后执行保留最近三版；旧版仍可查，解除固定后再次清理才回收其独占文件。",
  },
};

function money(cents) {
  if (cents === null || cents === undefined) return "—";
  return `¥${(Number(cents) / 100).toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function integer(value) {
  return value === null || value === undefined
    ? "—"
    : Number(value).toLocaleString("zh-CN");
}

function bytes(value) {
  if (value === null || value === undefined) return "—";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / 1024 / 1024).toFixed(2)} MiB`;
}

function time(value) {
  if (!value) return "—";
  const date = new Date(value);
  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function basename(path) {
  return path?.split("/").pop() || path;
}

function shortHash(hash) {
  return hash ? `${hash.slice(0, 10)}…${hash.slice(-6)}` : "—";
}

function operationLabel(operation) {
  return (
    {
      bootstrap: "初始写入",
      append: "追加写入",
      compact: "合并发布",
    }[operation] || operation
  );
}

const STATUS = {
  running: { label: "运行中", tone: "warning" },
  completed: { label: "已发布", tone: "good" },
  completed_noop: { label: "无需合并", tone: "neutral" },
  failed: { label: "失败", tone: "critical" },
  interrupted: { label: "进程中断", tone: "serious" },
  planned: { label: "已规划", tone: "neutral" },
  validated: { label: "校验通过", tone: "good" },
  validated_waiting: { label: "等待发布", tone: "warning" },
  conflict: { label: "版本冲突", tone: "serious" },
  published: { label: "已发布", tone: "good" },
  error: { label: "失败", tone: "critical" },
};

function StatusChip({ status, text }) {
  const meta = STATUS[status] || { label: status, tone: "neutral" };
  return (
    <span className={`status-chip tone-${meta.tone}`}>
      <span className="status-dot" aria-hidden="true" />
      {text || meta.label}
    </span>
  );
}

function CheckLine({ label, ok, detail }) {
  return (
    <div className={`check-line ${ok ? "is-ok" : "is-bad"}`}>
      <span className="check-icon" aria-hidden="true">
        {ok ? "✓" : "!"}
      </span>
      <span>{label}</span>
      {detail ? <span className="check-detail">{detail}</span> : null}
    </div>
  );
}

function Metric({ label, value, suffix, hint, emphasis }) {
  return (
    <div className={`metric ${emphasis ? "metric-emphasis" : ""}`}>
      <span className="metric-label">{label}</span>
      <strong className="metric-value">
        {value}
        {suffix ? <small>{suffix}</small> : null}
      </strong>
      {hint ? <span className="metric-hint">{hint}</span> : null}
    </div>
  );
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.message || data.detail || `HTTP ${response.status}`);
    error.status = response.status;
    error.payload = data;
    throw error;
  }
  return data;
}

function SnapshotResult({ snapshot }) {
  if (!snapshot) return null;
  const old = snapshot.snapshot_version !== snapshot.current_version;
  return (
    <article className="snapshot-result">
      <div className="snapshot-head">
        <div>
          <span className="eyebrow">固定快照</span>
          <h3>v{snapshot.snapshot_version}</h3>
        </div>
        <StatusChip
          status={snapshot.complete ? "completed" : "failed"}
          text={snapshot.complete ? "校验完整" : "校验异常"}
        />
      </div>
      {old ? (
        <div className="snapshot-note">
          <span className="snapshot-note-mark">SNAP</span>
          查询开始时固定 v{snapshot.snapshot_version}，读取期间 current 为 v
          {snapshot.current_version}。旧版引用文件未被改写。
        </div>
      ) : (
        <div className="snapshot-note">
          这是查询开始时的 current 快照；行数、金额与 manifest 逐项一致。
        </div>
      )}
      <div className="snapshot-metrics">
        <Metric label="行数" value={integer(snapshot.query.rows)} />
        <Metric label="金额" value={money(snapshot.query.total_amount_cents)} />
        <Metric label="文件数" value={integer(snapshot.files.length)} />
        <Metric label="DuckDB 耗时" value={snapshot.query_ms} suffix=" ms" />
      </div>
      <div className="check-grid">
        <CheckLine label="文件均存在" ok={snapshot.checks.files_exist} />
        <CheckLine label="文件数一致" ok={snapshot.checks.file_count_matches} />
        <CheckLine label="行数一致" ok={snapshot.checks.row_count_matches} />
        <CheckLine label="金额一致" ok={snapshot.checks.amount_matches} />
        <CheckLine label="无重复 event_id" ok={snapshot.checks.no_duplicate_event_ids} />
      </div>
      <details className="raw-details">
        <summary>引用文件与逐文件对账（{snapshot.files.length}）</summary>
        <div className="mini-table-wrap">
          <table className="mini-table">
            <thead>
              <tr>
                <th>文件</th>
                <th>类型</th>
                <th className="num">行数</th>
                <th className="num">金额</th>
                <th>实际读取</th>
              </tr>
            </thead>
            <tbody>
              {snapshot.per_file.map((file) => {
                const rowMatch = file.actual_rows === file.rows;
                const amountMatch = file.actual_amount_cents === file.amount_cents;
                return (
                  <tr key={file.path}>
                    <td title={file.path}>{basename(file.path)}</td>
                    <td>
                      <span className="kind">{file.kind}</span>
                    </td>
                    <td className="num">{integer(file.rows)}</td>
                    <td className="num">{money(file.amount_cents)}</td>
                    <td>
                      <span className={`tiny-check ${rowMatch && amountMatch ? "is-ok" : "is-bad"}`}>
                        {rowMatch && amountMatch ? "一致" : "不一致"}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="hash-line mono">manifest SHA-256 · {shortHash(snapshot.manifest_sha256)}</p>
      </details>
    </article>
  );
}

function CompareResult({ comparison }) {
  if (!comparison) return null;
  const { old, current } = comparison;
  return (
    <article className="compare-result">
      <span className="eyebrow">快照对比</span>
      <div className="compare-columns">
        <div className="compare-col">
          <span className="compare-tag">v{old.snapshot_version} · 旧快照</span>
          <strong>{integer(old.query.rows)} 行</strong>
          <span>{money(old.query.total_amount_cents)}</span>
          <span className={`tiny-check ${old.complete ? "is-ok" : "is-bad"}`}>
            {old.complete ? "完整" : "异常"}
          </span>
        </div>
        <div className="compare-arrow" aria-hidden="true">
          →
        </div>
        <div className="compare-col">
          <span className="compare-tag">v{current.snapshot_version} · 当前</span>
          <strong>{integer(current.query.rows)} 行</strong>
          <span>{money(current.query.total_amount_cents)}</span>
          <span className={`tiny-check ${current.complete ? "is-ok" : "is-bad"}`}>
            {current.complete ? "完整" : "异常"}
          </span>
        </div>
      </div>
      <div className="delta-row">
        <span>行数差 <strong>+{integer(comparison.row_delta)}</strong></span>
        <span>金额差 <strong>{money(comparison.amount_delta_cents)}</strong></span>
        <span className={`tiny-check ${comparison.both_complete ? "is-ok" : "is-bad"}`}>
          {comparison.both_complete ? "两份均通过完整性校验" : "存在完整性异常"}
        </span>
      </div>
    </article>
  );
}

function ScenarioEvidence({ result }) {
  if (!result) return null;
  const scenario = SCENARIOS[result.scenario] || {
    title: result.scenario,
    description: "",
  };
  const checks = result.checks || {};
  const checkLabels = {
    append_did_not_get_lost: "追加数据未丢失",
    amount_exact: "总金额精确",
    old_snapshot_unchanged: "旧快照行数/金额未变",
    old_snapshot_still_complete: "旧快照仍完整可读",
    current_snapshot_complete: "当前快照完整",
    no_duplicates: "无重复 event_id",
    rows_unchanged: "总行数不变",
    amount_unchanged: "总金额不变",
    current_complete: "当前快照完整",
    no_version_published: "未发布新版本",
    current_version_unchanged: "current 未推进",
    pinned_version_survived_cleanup: "固定旧版未被清理",
    pinned_snapshot_readable_during_retention: "固定旧版仍可完整查询",
    pinned_files_were_protected: "固定版引用文件未被删除",
    pinned_version_pruned_after_unpin: "解除固定后旧版被清理",
    old_snapshot_rejected_after_unpin: "旧快照返回 410",
    exclusive_files_reclaimed: "独占文件已回收",
    rows_exact_after_all_operations: "全过程行数精确",
    amount_exact_after_all_operations: "全过程金额精确",
  };
  return (
    <section className="evidence-card" aria-live="polite">
      <div className="evidence-head">
        <div>
          <span className="eyebrow">演示证据</span>
          <h2>{scenario.title}</h2>
          <p>{scenario.description}</p>
        </div>
        <span className="evidence-stamp mono">{result.scenario}</span>
      </div>
      {result.before ? (
        <div className="evidence-metrics">
          <Metric label="演示前" value={integer(result.before.rows)} suffix=" 行" />
          <Metric
            label="演示后"
            value={integer(result.after?.rows)}
            suffix=" 行"
            hint={`${money(result.before.amount_cents)} → ${money(result.after?.amount_cents)}`}
          />
          {result.during_compaction_query ? (
            <Metric
              label="合并期间读旧版"
              value={`v${result.during_compaction_query.snapshot_version}`}
              hint={`${integer(result.during_compaction_query.rows)} 行 · ${money(
                result.during_compaction_query.amount_cents
              )}`}
            />
          ) : null}
          {result.old_snapshot_read_again ? (
            <Metric
              label="再次读旧版"
              value={`v${result.old_snapshot_read_again.snapshot_version}`}
              hint={`${integer(result.old_snapshot_read_again.rows)} 行 · 完整 ${
                result.old_snapshot_read_again.complete ? "✓" : "×"
              }`}
            />
          ) : null}
          {result.append ? (
            <Metric
              label="并发追加"
              value={`v${result.append.version_id}`}
              hint={`${integer(result.append.rows)} 行 · ${money(result.append.amount_cents)}`}
            />
          ) : null}
          {result.conflict_count !== undefined ? (
            <Metric label="检测到冲突" value={integer(result.conflict_count)} suffix=" 次" />
          ) : null}
          {result.pinned_version ? (
            <Metric
              label="固定旧版"
              value={`v${result.pinned_version}`}
              hint={`${integer(result.pinned_version_rows)} 行 · 清理后仍可查`}
            />
          ) : null}
          {result.reclaimed_exclusive_files ? (
            <Metric
              label="解除固定后回收"
              value={integer(result.reclaimed_exclusive_files.length)}
              suffix=" 个文件"
              hint={result.old_query_status === 410 ? "旧快照返回 410" : ""}
            />
          ) : null}
        </div>
      ) : null}
      {result.conflict_attempts?.length ? (
        <div className="conflict-strip">
          <span>冲突轨迹</span>
          {result.conflict_attempts.map((item, index) => (
            <span key={`${item.task_id}-${index}`} className="mono">
              任务 {item.task_id.slice(0, 6)} · 尝试 #{item.attempt} · 基于 v
              {item.base_version} → 当前 v{item.current_version}
            </span>
          ))}
        </div>
      ) : null}
      {Object.keys(checks).length ? (
        <div className="check-grid evidence-checks">
          {Object.entries(checks).map(([key, value]) => (
            <CheckLine key={key} label={checkLabels[key] || key} ok={Boolean(value)} />
          ))}
        </div>
      ) : null}
      {result.skipped ? (
        <div className="inline-alert">{result.reason}</div>
      ) : null}
      {result.published_version === null && result.scenario === "failure-before-publish" ? (
        <div className="inline-alert">任务失败且未产生版本；临时输出已被隔离删除。</div>
      ) : null}
      <details className="raw-details">
        <summary>查看原始证据 JSON</summary>
        <pre className="raw-json">{JSON.stringify(result, null, 2)}</pre>
      </details>
    </section>
  );
}

function TaskCard({ task, expanded, onToggle }) {
  const attempts = task.attempts || [];
  const conflicts = attempts.filter((attempt) => attempt.status === "conflict").length;
  const latest = attempts[attempts.length - 1];
  return (
    <article className={`task-card ${expanded ? "is-open" : ""}`}>
      <button className="task-head" onClick={onToggle} aria-expanded={expanded}>
        <span className="task-title">
          <StatusChip status={task.status} />
          <span className="mono task-id">{task.id.slice(0, 9)}</span>
          <span className="task-label">{task.request?.label || "手动合并"}</span>
        </span>
        <span className="task-meta">
          {task.published_version ? `发布 v${task.published_version}` : null}
          {conflicts ? ` · ${conflicts} 次冲突` : null}
          <span className="mono">{time(task.created_at)}</span>
          <span className="chevron" aria-hidden="true">
            {expanded ? "−" : "+"}
          </span>
        </span>
      </button>
      {expanded ? (
        <div className="task-body">
          <div className="task-facts">
            <span>最多选 {task.request?.max_files} 个文件</span>
            <span>发布延迟 {task.request?.publish_delay_ms} ms</span>
            <span>尝试 {attempts.length} 次</span>
            {task.request?.simulate_failure ? <span>注入发布前失败</span> : null}
          </div>
          {attempts.map((attempt) => (
            <div className="attempt" key={`${attempt.attempt}-${attempt.started_at}`}>
              <div className="attempt-head">
                <span className="attempt-index">#{attempt.attempt}</span>
                <span>base v{attempt.base_version}</span>
                <StatusChip status={attempt.status} />
                <span>{attempt.input_file_count} 个输入文件</span>
                <span>{integer(attempt.input_rows)} 行</span>
                <span>{money(attempt.input_amount_cents)}</span>
                {attempt.conflict_current_version ? (
                  <span className="conflict-note">
                    发布时已是 v{attempt.conflict_current_version}
                  </span>
                ) : null}
              </div>
              {attempt.output_path ? (
                <div className="attempt-output mono">
                  输出 {basename(attempt.output_path)} · {integer(attempt.output_rows)} 行 ·
                  差异 {attempt.validation?.difference_rows ?? "—"} 行
                </div>
              ) : null}
              {attempt.error ? <div className="inline-alert">{attempt.error}</div> : null}
              <details className="attempt-files">
                <summary>本轮选中的文件（{attempt.selected_files?.length || 0}）</summary>
                <div className="mini-table-wrap">
                  <table className="mini-table">
                    <thead>
                      <tr>
                        <th>文件</th>
                        <th>类型</th>
                        <th className="num">行数</th>
                        <th className="num">金额</th>
                        <th className="num">大小</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(attempt.selected_files || []).map((file) => (
                        <tr key={file.path}>
                          <td title={file.path}>{basename(file.path)}</td>
                          <td>
                            <span className="kind">{file.kind}</span>
                          </td>
                          <td className="num">{integer(file.rows)}</td>
                          <td className="num">{money(file.amount_cents)}</td>
                          <td className="num">{bytes(file.bytes)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
            </div>
          ))}
          {latest?.validation ? (
            <div className="validation-strip">
              <span className="tiny-check is-ok">校验通过</span>
              <span>{latest.validation.method}</span>
              <span>前后差异 {latest.validation.difference_rows} 行</span>
            </div>
          ) : null}
          <div className="timeline">
            {(task.events || []).map((event, index) => (
              <div className={`timeline-item level-${event.level}`} key={`${event.at}-${index}`}>
                <span className="timeline-dot" aria-hidden="true" />
                <div>
                  <span className="timeline-time mono">{time(event.at)}</span>
                  <p>{event.message}</p>
                  {event.detail ? (
                    <span className="timeline-detail mono">
                      {Object.entries(event.detail)
                        .slice(0, 5)
                        .map(([key, value]) => `${key}=${String(value)}`)
                        .join(" · ")}
                    </span>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </article>
  );
}

function Distribution({ distribution, files, stagingCount = 0 }) {
  const total = Math.max(1, distribution.files);
  const groups = [
    {
      key: "small",
      label: "零散文件",
      count: distribution.small,
      color: "var(--series-1)",
    },
    {
      key: "compacted",
      label: "已合并大文件",
      count: distribution.compacted,
      color: "var(--series-3)",
    },
    {
      key: "other",
      label: "其他/未登记",
      count: Math.max(0, distribution.files - distribution.small - distribution.compacted),
      color: "var(--series-2)",
    },
  ];
  return (
    <section className="card files-card">
      <div className="section-head">
        <div>
          <span className="eyebrow">分区文件分布</span>
          <h2>物理文件与引用关系</h2>
        </div>
        <div className="files-summary">
          <span>{distribution.files} 个文件</span>
          <span>{bytes(distribution.bytes)}</span>
          {stagingCount ? (
            <StatusChip
              status="running"
              text={`${stagingCount} 个临时文件 · 查询不可见`}
            />
          ) : null}
          {distribution.orphan ? (
            <StatusChip status="failed" text={`${distribution.orphan} 个未引用`} />
          ) : (
            <StatusChip status="completed" text="全部有引用" />
          )}
        </div>
      </div>
      <div className="viz-root distribution-viz">
        <div
          className="distribution-bar"
          role="img"
          aria-label={`文件分布：零散 ${distribution.small}，已合并 ${distribution.compacted}，其他 ${
            distribution.files - distribution.small - distribution.compacted
          }`}
        >
          {groups.map((group) =>
            group.count > 0 ? (
              <span
                key={group.key}
                className="distribution-segment"
                style={{
                  width: `${(group.count / total) * 100}%`,
                  background: group.color,
                }}
                title={`${group.label}: ${group.count} 个文件`}
              />
            ) : null
          )}
        </div>
        <div className="distribution-legend">
          {groups.map((group) => (
            <span key={group.key} className="legend-item">
              <span className="legend-swatch" style={{ background: group.color }} />
              {group.label}
              <strong>{group.count}</strong>
            </span>
          ))}
          <span className="legend-item legend-note">
            当前版本引用 {distribution.current_files} 个
          </span>
        </div>
      </div>
      <div className="table-wrap files-table-wrap">
        <table>
          <thead>
            <tr>
              <th>文件</th>
              <th>类型</th>
              <th className="num">行数</th>
              <th className="num">大小</th>
              <th>被哪些版本引用</th>
              <th>状态</th>
            </tr>
          </thead>
          <tbody>
            {files.map((file) => (
              <tr key={file.path} className={file.orphan ? "row-warn" : ""}>
                <td title={file.path} className="file-cell mono">
                  {basename(file.path)}
                </td>
                <td>
                  <span className="kind">{file.kind}</span>
                </td>
                <td className="num">{integer(file.rows)}</td>
                <td className="num">{bytes(file.bytes)}</td>
                <td>
                  <span className="version-refs">
                    {(file.referenced_by || []).slice(0, 6).map((version) => (
                      <span key={version} className="version-ref mono">
                        v{version}
                      </span>
                    ))}
                    {(file.referenced_by || []).length > 6 ? (
                      <span className="version-ref mono">
                        +{file.referenced_by.length - 6}
                      </span>
                    ) : null}
                    {!file.referenced_by?.length ? <span className="muted">无</span> : null}
                  </span>
                </td>
                <td>
                  {file.referenced_by_current ? (
                    <StatusChip status="completed" text="current" />
                  ) : file.orphan ? (
                    <StatusChip status="serious" text="未引用" />
                  ) : (
                    <StatusChip status="running" text="历史版本" />
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default function App() {
  const [state, setState] = useState(null);
  const [loadError, setLoadError] = useState(null);
  const [busy, setBusy] = useState(null);
  const [notice, setNotice] = useState(null);
  const [selectedVersion, setSelectedVersion] = useState(null);
  const [snapshot, setSnapshot] = useState(null);
  const [comparison, setComparison] = useState(null);
  const [verification, setVerification] = useState(null);
  const [scenarioResult, setScenarioResult] = useState(null);
  const [cleanupResult, setCleanupResult] = useState(null);
  const [expandedTask, setExpandedTask] = useState(null);
  const [appendRows, setAppendRows] = useState(24);
  const [maxFiles, setMaxFiles] = useState(16);
  const [delayMs, setDelayMs] = useState(1200);
  const initialized = useRef(false);

  const refresh = async () => {
    try {
      const data = await api("/api/state");
      setState(data);
      setLoadError(null);
      if (!initialized.current && data.current_version !== null) {
        initialized.current = true;
        setSelectedVersion(data.current_version);
      }
    } catch (error) {
      setLoadError(error.message);
    }
  };

  useEffect(() => {
    refresh();
    const handle = setInterval(refresh, 1800);
    return () => clearInterval(handle);
  }, []);

  useEffect(() => {
    if (!notice) return undefined;
    const handle = setTimeout(() => setNotice(null), 6000);
    return () => clearTimeout(handle);
  }, [notice]);

  const runningCount = useMemo(
    () => (state?.tasks || []).filter((task) => task.status === "running").length,
    [state]
  );

  const versions = state?.versions || [];
  const currentVersion = state?.current_version;

  const run = async (name, action, successMessage) => {
    setBusy(name);
    setNotice(null);
    try {
      const result = await action();
      if (successMessage) {
        setNotice({ tone: "good", message: successMessage });
      }
      await refresh();
      return result;
    } catch (error) {
      setNotice({ tone: "bad", message: error.message });
      return null;
    } finally {
      setBusy(null);
    }
  };

  const handleAppend = async () => {
    const result = await run(
      "append",
      () =>
        api("/api/append", {
          method: "POST",
          body: JSON.stringify({ rows: Number(appendRows) }),
        }),
      "追加文件已写入并通过校验"
    );
    if (result) {
      setSelectedVersion(result.version.version_id);
      setSnapshot(null);
      setComparison(null);
    }
  };

  const handleCompaction = async (payload = {}) => {
    const task = await run(
      "compact",
      () =>
        api("/api/compactions", {
          method: "POST",
          body: JSON.stringify({
            max_files: Number(maxFiles),
            publish_delay_ms: Number(delayMs),
            ...payload,
          }),
        }),
      "合并任务已提交"
    );
    if (task) {
      setExpandedTask(task.id);
      setScenarioResult(null);
    }
  };

  const runScenario = async (key) => {
    const scenario = SCENARIOS[key];
    const payload =
      key === "append"
        ? { max_files: 12, publish_delay_ms: Math.max(Number(delayMs), 800) }
        : key === "dual"
          ? { max_files: Number(maxFiles), publish_delay_ms: Math.max(Number(delayMs), 900) }
          : key === "failure"
            ? { max_files: Number(maxFiles), publish_delay_ms: 250 }
            : key === "pinGc"
              ? { max_files: 64 }
              : { max_files: Number(maxFiles) };
    setScenarioResult(null);
    const result = await run(
      `scenario-${key}`,
      () =>
        api(scenario.path, {
          method: "POST",
          body: JSON.stringify(payload),
        }),
      `${scenario.title} 已完成`
    );
    if (result) {
      setScenarioResult(result);
      if (result.cleanup_after_unpin) {
        setCleanupResult(result.cleanup_after_unpin);
      }
      if (result.after?.current_version) {
        setSelectedVersion(result.after.current_version);
      }
      await refresh();
    }
  };

  const querySnapshot = async () => {
    if (selectedVersion === null) return;
    const result = await run(
      "snapshot",
      () => api(`/api/snapshot?version=${selectedVersion}`),
      null
    );
    if (result) {
      setSnapshot(result);
      setComparison(null);
      setVerification(null);
    }
  };

  const compareCurrent = async () => {
    if (selectedVersion === null) return;
    const result = await run(
      "compare",
      () => api(`/api/compare?version=${selectedVersion}`),
      null
    );
    if (result) {
      setComparison(result);
      setSnapshot(result.old);
      setVerification(null);
    }
  };

  const verifyVersion = async () => {
    if (selectedVersion === null) return;
    const result = await run(
      "verify",
      () =>
        api(`/api/versions/${selectedVersion}/verify`, {
          method: "POST",
          body: "{}",
        }),
      null
    );
    if (result) setVerification(result);
  };

  const togglePin = async (version) => {
    if (version.pinned) {
      await run(
        `unpin-${version.version_id}`,
        () =>
          api(`/api/versions/${version.version_id}/unpin`, {
            method: "POST",
            body: "{}",
          }),
        `v${version.version_id} 已解除固定`
      );
    } else {
      await run(
        `pin-${version.version_id}`,
        () =>
          api(`/api/versions/${version.version_id}/pin`, {
            method: "POST",
            body: JSON.stringify({ note: "页面手动固定" }),
          }),
        `v${version.version_id} 已固定，清理时不会被删除`
      );
    }
  };

  const handleCleanup = async () => {
    const result = await run(
      "cleanup",
      () =>
        api("/api/cleanup", {
          method: "POST",
          body: JSON.stringify({ keep: 3 }),
        }),
      "清理完成：仅删除了不被保留版本引用的文件"
    );
    if (result) {
      setCleanupResult(result);
      setScenarioResult(null);
      if (result.state?.current_version) {
        setSelectedVersion(result.state.current_version);
      }
    }
  };

  const selectVersion = (version) => {
    setSelectedVersion(version);
    setSnapshot(null);
    setComparison(null);
    setVerification(null);
  };

  if (!state) {
    return (
      <div className="boot-screen">
        <div className="boot-mark">
          <i />
          <i />
          <i />
          <i />
        </div>
        <h1>Parquet Snapshot Studio</h1>
        <p>{loadError || "正在读取 catalog 与 manifest…"}</p>
        {loadError ? (
          <button className="btn" onClick={refresh}>
            重试
          </button>
        ) : null}
      </div>
    );
  }

  const aggregates = state.current_manifest_aggregates || { rows: 0, amount_cents: 0, file_count: 0 };
  const tasks = state.tasks || [];
  const pins = state.pins || [];

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <i />
            <i />
            <i />
            <i />
          </div>
          <div>
            <span className="brand-kicker">PARQUET / SNAPSHOT / MANIFEST</span>
            <h1>Parquet Snapshot Studio</h1>
          </div>
        </div>
        <div className="topbar-meta">
          <span className="live-dot" aria-hidden="true" />
          <span>catalog 在线</span>
          <span className="divider" />
          <span className="mono path" title={state.config.data_root}>
            {state.config.data_root}
          </span>
          <button
            className="icon-btn"
            onClick={refresh}
            title="刷新状态"
            aria-label="刷新状态"
          >
            ↻
          </button>
        </div>
      </header>

      <main>
        {notice ? (
          <div className={`toast toast-${notice.tone}`} role="status" aria-live="polite">
            {notice.message}
            <button onClick={() => setNotice(null)} aria-label="关闭">
              ×
            </button>
          </div>
        ) : null}

        <section className="hero">
          <div className="hero-copy">
            <span className="eyebrow">单一分区 · immutable files · optimistic publish</span>
            <h2>
              零散文件持续合并，
              <br />
              查询始终握住自己的那一版。
            </h2>
            <p>
              每个 manifest 是一个不可变版本，只有通过 PyArrow / DuckDB 完整校验后才原子发布。
              合并只替换本轮选中的文件；并发追加或另一轮发布会让旧任务冲突并重新规划。
            </p>
          </div>
          <div className="overview-grid">
            <Metric
              label="当前版本"
              value={`v${currentVersion}`}
              hint={`${versions.length} 个版本 · ${pins.length} 个固定`}
              emphasis
            />
            <Metric
              label="当前文件"
              value={integer(aggregates.file_count)}
              suffix=" 个"
              hint={
                state.distribution.compacted
                  ? `${state.distribution.small} 零散 / ${state.distribution.compacted} 大文件`
                  : "全部为零散文件"
              }
            />
            <Metric
              label="当前行数"
              value={integer(aggregates.rows)}
              hint="按 current manifest 记录"
            />
            <Metric
              label="当前金额"
              value={money(aggregates.amount_cents)}
              hint="分币整数汇总，无浮点误差"
            />
          </div>
        </section>

        <section className="operations card">
          <div className="section-head">
            <div>
              <span className="eyebrow">操作台</span>
              <h2>写入、合并与故障演示</h2>
            </div>
            <span className="ops-status">
              {runningCount ? (
                <StatusChip status="running" text={`${runningCount} 个任务运行中`} />
              ) : (
                <StatusChip status="completed" text="无运行中任务" />
              )}
              <span className="mono">keep {state.config.keep_versions}</span>
            </span>
          </div>
          <div className="control-grid">
            <div className="control">
              <label htmlFor="append-rows">追加行数</label>
              <input
                id="append-rows"
                type="number"
                min="1"
                max="10000"
                value={appendRows}
                onChange={(event) => setAppendRows(event.target.value)}
              />
              <button className="btn btn-primary" onClick={handleAppend} disabled={Boolean(busy)}>
                {busy === "append" ? "写入中…" : "追加新文件"}
              </button>
            </div>
            <div className="control">
              <label htmlFor="max-files">每轮最多选择文件</label>
              <input
                id="max-files"
                type="number"
                min="2"
                max="64"
                value={maxFiles}
                onChange={(event) => setMaxFiles(event.target.value)}
              />
              <button className="btn" onClick={() => handleCompaction()} disabled={Boolean(busy)}>
                {busy === "compact" ? "提交中…" : "发起合并"}
              </button>
            </div>
            <div className="control">
              <label htmlFor="delay-ms">发布延迟</label>
              <input
                id="delay-ms"
                type="number"
                min="0"
                max="15000"
                step="100"
                value={delayMs}
                onChange={(event) => setDelayMs(event.target.value)}
              />
              <span className="control-hint">给并发写入留出窗口</span>
            </div>
          </div>
          <div className="demo-grid">
            {Object.entries(SCENARIOS).map(([key, scenario]) => (
              <button
                key={key}
                className="demo-btn"
                onClick={() => runScenario(key)}
                disabled={Boolean(busy)}
              >
                <span className="demo-index">{String(Object.keys(SCENARIOS).indexOf(key) + 1).padStart(2, "0")}</span>
                <span>
                  <strong>{scenario.title}</strong>
                  <small>{scenario.description}</small>
                </span>
                {busy === `scenario-${key}` ? <span className="demo-running">运行中…</span> : null}
              </button>
            ))}
          </div>
        </section>

        <ScenarioEvidence result={scenarioResult} />

        <div className="main-grid">
          <div className="stack">
            <section className="card versions-card">
              <div className="section-head">
                <div>
                  <span className="eyebrow">版本列表</span>
                  <h2>Manifest 版本链</h2>
                </div>
                <span className="muted">固定旧版可在清理后继续查询</span>
              </div>
              <div className="table-wrap versions-table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>版本</th>
                      <th>操作</th>
                      <th className="num">文件</th>
                      <th className="num">行数</th>
                      <th className="num">金额</th>
                      <th>创建时间</th>
                      <th>状态</th>
                      <th className="actions-col">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {versions.map((version) => (
                      <tr
                        key={version.version_id}
                        className={[
                          version.version_id === currentVersion ? "row-current" : "",
                          version.pruned ? "row-pruned" : "",
                          selectedVersion === version.version_id ? "row-selected" : "",
                        ].join(" ")}
                      >
                        <td>
                          <button
                            className="version-link mono"
                            onClick={() => selectVersion(version.version_id)}
                          >
                            v{version.version_id}
                            {version.version_id === currentVersion ? (
                              <span className="current-dot" title="current" />
                            ) : null}
                          </button>
                        </td>
                        <td>
                          <span className={`op-tag op-${version.operation}`}>
                            {operationLabel(version.operation)}
                          </span>
                        </td>
                        <td className="num">{integer(version.file_count)}</td>
                        <td className="num">{integer(version.rows)}</td>
                        <td className="num">{money(version.amount_cents)}</td>
                        <td className="mono time-cell">{time(version.created_at)}</td>
                        <td>
                          <span className="status-stack">
                            {version.pruned ? (
                              <StatusChip status="failed" text="已清理" />
                            ) : (
                              <StatusChip status="completed" text="已校验" />
                            )}
                            {version.pinned ? (
                              <StatusChip status="running" text="已固定" />
                            ) : null}
                          </span>
                        </td>
                        <td>
                          <div className="row-actions">
                            <button
                              className="link-btn"
                              onClick={() => selectVersion(version.version_id)}
                              disabled={version.pruned}
                            >
                              查看
                            </button>
                            <button
                              className="link-btn"
                              onClick={() => togglePin(version)}
                              disabled={version.pruned || Boolean(busy)}
                            >
                              {version.pinned ? "解除固定" : "固定"}
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="card tasks-card">
              <div className="section-head">
                <div>
                  <span className="eyebrow">合并任务</span>
                  <h2>尝试、冲突与发布轨迹</h2>
                </div>
                <span className="muted">展开可查看本轮引用文件</span>
              </div>
              <div className="task-list">
                {tasks.length ? (
                  tasks.slice(0, 8).map((task) => (
                    <TaskCard
                      key={task.id}
                      task={task}
                      expanded={expandedTask === task.id}
                      onToggle={() =>
                        setExpandedTask(expandedTask === task.id ? null : task.id)
                      }
                    />
                  ))
                ) : (
                  <div className="empty-state">
                    还没有合并任务。发起一次合并，或运行上方的冲突演示。
                  </div>
                )}
              </div>
            </section>
          </div>

          <div className="stack side-stack">
            <section className="card snapshot-lab">
              <div className="section-head">
                <div>
                  <span className="eyebrow">快照查询</span>
                  <h2>固定一版，慢慢查</h2>
                </div>
                <span className="mono snap-icon">SNAP</span>
              </div>
              <p className="section-copy">
                查询开始时在 catalog 中登记版本租约；DuckDB 只读取该 manifest
                列出的不可变文件。合并发布不会改变正在读取的旧版本。
              </p>
              <div className="snapshot-controls">
                <label htmlFor="version-select">选择版本</label>
                <select
                  id="version-select"
                  value={selectedVersion ?? ""}
                  onChange={(event) => {
                    selectVersion(Number(event.target.value));
                  }}
                >
                  {versions
                    .filter((version) => !version.pruned)
                    .map((version) => (
                      <option key={version.version_id} value={version.version_id}>
                        v{version.version_id} · {operationLabel(version.operation)}
                        {version.version_id === currentVersion ? " · current" : ""}
                        {version.pinned ? " · pinned" : ""}
                      </option>
                    ))}
                </select>
                <button
                  className="btn btn-primary"
                  onClick={querySnapshot}
                  disabled={Boolean(busy) || selectedVersion === null}
                >
                  {busy === "snapshot" ? "查询中…" : "查询快照"}
                </button>
                <button
                  className="btn"
                  onClick={compareCurrent}
                  disabled={Boolean(busy) || selectedVersion === null}
                >
                  {busy === "compare" ? "对比中…" : "与 current 对比"}
                </button>
                <button
                  className="btn btn-quiet"
                  onClick={verifyVersion}
                  disabled={Boolean(busy) || selectedVersion === null}
                >
                  {busy === "verify" ? "校验中…" : "校验文件哈希"}
                </button>
              </div>
              {selectedVersion !== null && selectedVersion !== currentVersion ? (
                <div className="old-version-hint">
                  正在查看旧版本。若清理时未固定，且它不在最近三版内，manifest
                  与独占文件将被回收。
                </div>
              ) : null}
              {snapshot ? <SnapshotResult snapshot={snapshot} /> : null}
              {comparison ? <CompareResult comparison={comparison} /> : null}
              {verification ? (
                <article className="verification-result">
                  <div className="snapshot-head">
                    <div>
                      <span className="eyebrow">Manifest 完整性</span>
                      <h3>v{verification.version}</h3>
                    </div>
                    <StatusChip
                      status={verification.valid ? "completed" : "failed"}
                      text={verification.valid ? "哈希与汇总均一致" : "发现异常"}
                    />
                  </div>
                  <div className="check-grid">
                    <CheckLine label="汇总行数一致" ok={verification.aggregate_matches} />
                    {verification.files.map((file) => (
                      <CheckLine
                        key={file.path}
                        label={basename(file.path)}
                        ok={file.valid}
                        detail={`${integer(file.actual?.rows)} 行`}
                      />
                    ))}
                  </div>
                </article>
              ) : null}
            </section>

            <section className="card cleanup-card">
              <div className="section-head">
                <div>
                  <span className="eyebrow">保留与回收</span>
                  <h2>最近三版 + 固定版本</h2>
                </div>
                <span className="mono">GC</span>
              </div>
              <div className="cleanup-rules">
                <div>
                  <strong>01</strong>
                  <span>最近 3 个已发布版本始终保留</span>
                </div>
                <div>
                  <strong>02</strong>
                  <span>页面固定的旧版本及其引用文件不删除</span>
                </div>
                <div>
                  <strong>03</strong>
                  <span>先提交 pruned 元数据，再删除独占文件</span>
                </div>
              </div>
              <div className="pin-list">
                {pins.length ? (
                  pins.map((pin) => (
                    <div className="pin-row" key={pin.id}>
                      <span className="pin-version mono">v{pin.version_id}</span>
                      <span className="pin-note">{pin.note || "已固定"}</span>
                      <button
                        className="link-btn"
                        onClick={() => {
                          const version = versions.find(
                            (item) => item.version_id === pin.version_id
                          );
                          if (version) togglePin(version);
                        }}
                        disabled={Boolean(busy)}
                      >
                        解除固定
                      </button>
                    </div>
                  ))
                ) : (
                  <span className="muted">暂无固定版本</span>
                )}
              </div>
              <button
                className="btn btn-danger"
                onClick={handleCleanup}
                disabled={Boolean(busy)}
              >
                {busy === "cleanup" ? "清理中…" : "执行清理 · 保留最近 3 版"}
              </button>
              {cleanupResult ? (
                <div className="cleanup-result">
                  <div className="cleanup-stats">
                    <Metric label="删除文件" value={integer(cleanupResult.deleted_files.length)} suffix=" 个" />
                    <Metric label="回收空间" value={bytes(cleanupResult.deleted_bytes)} />
                    <Metric label="保留文件引用" value={integer(cleanupResult.kept_file_count)} />
                  </div>
                  <div className="cleanup-protected">
                    {cleanupResult.protected_versions.map((item) => (
                      <span key={item.version_id} className="version-ref mono">
                        v{item.version_id} · {item.reason}
                      </span>
                    ))}
                  </div>
                  {cleanupResult.pruned_versions.length ? (
                    <p className="cleanup-note">
                      已清理版本：{cleanupResult.pruned_versions.map((v) => `v${v}`).join("、")}
                    </p>
                  ) : (
                    <p className="cleanup-note">没有可清理的旧版本。</p>
                  )}
                  {cleanupResult.deleted_files.length ? (
                    <details className="raw-details">
                      <summary>回收文件清单</summary>
                      <ul className="simple-list mono">
                        {cleanupResult.deleted_files.map((file, index) => (
                          <li key={`${file.path}-${index}`}>
                            {basename(file.path)} · {bytes(file.bytes)}
                          </li>
                        ))}
                      </ul>
                    </details>
                  ) : null}
                </div>
              ) : null}
            </section>
          </div>
        </div>

        <Distribution
          distribution={state.distribution}
          files={state.files}
          stagingCount={state.staging_files.length}
        />

        <section className="card architecture-card">
          <div className="architecture-copy">
            <span className="eyebrow">写入不变量</span>
            <h2>为什么并发下不会多一份、少一份</h2>
          </div>
          <div className="architecture-grid">
            <div>
              <span className="arch-num">A</span>
              <h3>文件写入后只读</h3>
              <p>Parquet 路径唯一，发布后 chmod 为只读。合并只产生新文件，不修改旧字节。</p>
            </div>
            <div>
              <span className="arch-num">B</span>
              <h3>Manifest 完整引用</h3>
              <p>每版记录文件列表、行数、金额、哈希；current 指针只在校验后原子切换。</p>
            </div>
            <div>
              <span className="arch-num">C</span>
              <h3>乐观版本冲突</h3>
              <p>发布事务比较 base 与 current。追加或另一轮合并推进版本时，本轮输出被丢弃并重新规划。</p>
            </div>
            <div>
              <span className="arch-num">D</span>
              <h3>引用感知回收</h3>
              <p>清理先保护最近三版、固定版、活动查询租约和运行中任务，再删除无保留引用的文件。</p>
            </div>
          </div>
        </section>

        <footer>
          <span className="mono">PYARROW · DUCKDB · FASTAPI · REACT</span>
          <span>
            分区 {state.partition} · staging {state.staging_files.length} 个临时文件 ·
            {runningCount ? ` ${runningCount} 个合并任务运行中` : " 无运行中任务"}
          </span>
        </footer>
      </main>
    </div>
  );
}
