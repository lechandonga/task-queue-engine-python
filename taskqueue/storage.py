"""持久化层：单文件 SQLite，WAL 模式，所有状态变更走短事务。

线程模型：本存储对连接加锁串行化所有写操作（SQLite 默认连接跨线程需
``check_same_thread=False``），保证多 worker / 多消费者并发下的原子语义。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any

from taskqueue.errors import NotFoundError
from taskqueue.models import DeadLetter, EventRecord, TaskRecord, TaskStatus

_TASK_TERMINAL = (TaskStatus.COMPLETED.value, TaskStatus.DEAD.value,
                  TaskStatus.CANCELLED.value)


class Storage:
    """SQLite 持久化存储。所有方法要么原子提交，要么抛错不留半成品。"""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 ------------------------------------------------------
    def open(self) -> None:
        with self._lock:
            if self._conn is not None:
                return
            conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                isolation_level=None,  # 显式事务
                timeout=30.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=30000")
            self._conn = conn
            self._create_schema()

    def close(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            self._conn.close()
            self._conn = None

    def __enter__(self) -> Storage:
        self.open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _create_schema(self) -> None:
        assert self._conn is not None
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                queue TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                dedup_key TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                run_at_ms INTEGER NOT NULL,
                leased_by TEXT,
                lease_expires_ms INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_due
                ON tasks(namespace, queue, status, run_at_ms);
            CREATE INDEX IF NOT EXISTS idx_tasks_dedup
                ON tasks(namespace, dedup_key);

            CREATE TABLE IF NOT EXISTS dead_letters (
                task_id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                queue TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                reason TEXT NOT NULL,
                failed_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_ns
                ON dead_letters(namespace, queue);

            CREATE TABLE IF NOT EXISTS dedup_window (
                namespace TEXT NOT NULL,
                dedup_key TEXT NOT NULL,
                task_id TEXT NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, dedup_key)
            );

            CREATE TABLE IF NOT EXISTS events (
                namespace TEXT NOT NULL,
                topic TEXT NOT NULL,
                offset INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, topic, offset)
            );

            CREATE TABLE IF NOT EXISTS consumer_groups (
                namespace TEXT NOT NULL,
                topic TEXT NOT NULL,
                grp TEXT NOT NULL,
                committed_offset INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(namespace, topic, grp)
            );

            CREATE TABLE IF NOT EXISTS group_acks (
                namespace TEXT NOT NULL,
                topic TEXT NOT NULL,
                grp TEXT NOT NULL,
                offset INTEGER NOT NULL,
                status TEXT NOT NULL,
                leased_by TEXT,
                lease_expires_ms INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(namespace, topic, grp, offset)
            );
            """
        )

    # ---- 行映射 --------------------------------------------------------
    @staticmethod
    def _row_to_task(r: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=r["id"],
            namespace=r["namespace"],
            queue=r["queue"],
            payload=json.loads(r["payload"]),
            status=TaskStatus(r["status"]),
            attempt=r["attempt"],
            dedup_key=r["dedup_key"],
            created_at_ms=r["created_at_ms"],
            updated_at_ms=r["updated_at_ms"],
            run_at_ms=r["run_at_ms"],
            leased_by=r["leased_by"],
            lease_expires_ms=r["lease_expires_ms"],
            last_error=r["last_error"],
        )

    # ---- 任务 ----------------------------------------------------------
    def submit_task(self, task: TaskRecord, now: int, max_pending: int,
                    max_backlog_bytes: int,
                    window_ms: int | None = None) -> tuple[str, str]:
        """原子提交：在单个 IMMEDIATE 事务内完成“有界准入 + 去重判定 + 插入”。

        返回 ``(status, task_id)``：

        - ``("inserted", id)``：新任务落库（同时登记去重窗口）；
        - ``("dedup_hit", 首次任务 id)``：窗口内同 key 已存在，不占队列额度；
        - ``("full", "")``：待调度数量或积压字节超限（去重命中优先于 full，
          重复提交永远不会因为队列满而被拒）。

        先查窗口再查额度：重复提交直接短路返回，不被有界策略影响。
        """
        assert self._conn is not None
        payload_text = json.dumps(task.payload)
        payload_len = len(payload_text.encode("utf-8"))
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                if task.dedup_key is not None:
                    r = conn.execute(
                        "SELECT task_id, expires_at_ms FROM dedup_window "
                        "WHERE namespace=? AND dedup_key=?",
                        (task.namespace, task.dedup_key),
                    ).fetchone()
                    if r is not None and int(r["expires_at_ms"]) > now:
                        conn.execute("COMMIT")
                        return "dedup_hit", str(r["task_id"])
                    conn.execute(
                        "DELETE FROM dedup_window WHERE namespace=? AND dedup_key=?",
                        (task.namespace, task.dedup_key),
                    )

                cnt = int(conn.execute(
                    "SELECT COUNT(*) c FROM tasks WHERE namespace=? "
                    "AND status IN (?, ?)",
                    (task.namespace, TaskStatus.PENDING.value,
                     TaskStatus.RETRY_WAIT.value),
                ).fetchone()["c"])
                backlog = int(conn.execute(
                    "SELECT COALESCE(SUM(LENGTH(payload)),0) s FROM tasks "
                    "WHERE namespace=? AND status NOT IN (?, ?, ?)",
                    (task.namespace, *_TASK_TERMINAL),
                ).fetchone()["s"])
                if cnt >= max_pending or backlog + payload_len > max_backlog_bytes:
                    conn.execute("COMMIT")
                    return "full", ""

                conn.execute(
                    """INSERT INTO tasks(id, namespace, queue, payload, status, attempt,
                           dedup_key, created_at_ms, updated_at_ms, run_at_ms,
                           leased_by, lease_expires_ms, last_error)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        task.id, task.namespace, task.queue, payload_text,
                        task.status.value, task.attempt, task.dedup_key,
                        task.created_at_ms, task.updated_at_ms, task.run_at_ms,
                        task.leased_by, task.lease_expires_ms, task.last_error,
                    ),
                )
                if task.dedup_key is not None and window_ms is not None:
                    conn.execute(
                        "INSERT INTO dedup_window(namespace, dedup_key, task_id, expires_at_ms) "
                        "VALUES(?,?,?,?)",
                        (task.namespace, task.dedup_key, task.id, now + window_ms),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return "inserted", task.id

    def insert_task(self, task: TaskRecord) -> None:
        assert self._conn is not None
        with self._lock:
            self._conn.execute(
                """INSERT INTO tasks(id, namespace, queue, payload, status, attempt,
                       dedup_key, created_at_ms, updated_at_ms, run_at_ms,
                       leased_by, lease_expires_ms, last_error)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task.id, task.namespace, task.queue, json.dumps(task.payload),
                    task.status.value, task.attempt, task.dedup_key,
                    task.created_at_ms, task.updated_at_ms, task.run_at_ms,
                    task.leased_by, task.lease_expires_ms, task.last_error,
                ),
            )

    def get_task(self, task_id: str) -> TaskRecord | None:
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM tasks WHERE id=?", (task_id,)
            ).fetchone()
            return self._row_to_task(r) if r else None

    def require_task(self, task_id: str) -> TaskRecord:
        task = self.get_task(task_id)
        if task is None:
            raise NotFoundError(f"task {task_id} not found")
        return task

    def get_task_by_dedup(self, namespace: str, dedup_key: str) -> TaskRecord | None:
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM tasks WHERE namespace=? AND dedup_key=?",
                (namespace, dedup_key),
            ).fetchone()
            return self._row_to_task(r) if r else None

    def fetch_due_tasks(self, namespace: str, now: int, limit: int,
                        worker_id: str, lease_ms: int) -> list[TaskRecord]:
        """原子认领：把到期的 PENDING/RETRY_WAIT 任务置为 RUNNING 并加租约。

        单条 ``UPDATE ... WHERE id IN (SELECT ...)`` 完成挑选+占用，
        多 worker 并发不会重复认领。
        """
        assert self._conn is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    """SELECT id FROM tasks
                       WHERE (? = '%' OR namespace = ?)
                         AND status IN (?, ?)
                         AND run_at_ms <= ?
                       ORDER BY run_at_ms, created_at_ms
                       LIMIT ?""",
                    (namespace, namespace, TaskStatus.PENDING.value,
                     TaskStatus.RETRY_WAIT.value, now, limit),
                ).fetchall()
                ids = [r["id"] for r in rows]
                if ids:
                    placeholders = ",".join("?" * len(ids))
                    conn.execute(
                        f"""UPDATE tasks
                            SET status=?, leased_by=?, lease_expires_ms=?,
                                attempt=attempt+1, updated_at_ms=?
                            WHERE id IN ({placeholders})""",
                        [TaskStatus.RUNNING.value, worker_id, now + lease_ms, now, *ids],
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return [self.require_task(i) for i in ids]

    def complete_task(self, task_id: str, worker_id: str, now: int) -> bool:
        """仅当任务仍由 ``worker_id`` 持租约时才允许置为 COMPLETED。"""
        assert self._conn is not None
        with self._lock:
            cur = self._conn.execute(
                """UPDATE tasks
                   SET status=?, leased_by=NULL, lease_expires_ms=0,
                       updated_at_ms=?, last_error=NULL
                   WHERE id=? AND status=? AND leased_by=?""",
                (TaskStatus.COMPLETED.value, now, task_id,
                 TaskStatus.RUNNING.value, worker_id),
            )
            return cur.rowcount == 1

    def reschedule_task(self, task_id: str, worker_id: str, run_at_ms: int,
                        error: str, now: int) -> bool:
        """失败退避：持租约者把任务退回 RETRY_WAIT，等待下一轮认领。"""
        assert self._conn is not None
        with self._lock:
            cur = self._conn.execute(
                """UPDATE tasks
                   SET status=?, leased_by=NULL, lease_expires_ms=0,
                       run_at_ms=?, updated_at_ms=?, last_error=?
                   WHERE id=? AND status=? AND leased_by=?""",
                (TaskStatus.RETRY_WAIT.value, run_at_ms, now, error, task_id,
                 TaskStatus.RUNNING.value, worker_id),
            )
            return cur.rowcount == 1

    def fail_task_to_dead(self, task_id: str, reason: str, now: int,
                          worker_id: str | None = None) -> DeadLetter | None:
        """原子地把任务移入死信表（同事务 UPDATE+INSERT，不留中间态）。

        提供 ``worker_id`` 时仅当任务仍由该 worker 持租约才生效；
        租约已转移（被回收重投）则返回 None（调用方放弃迟到结果）。
        """
        assert self._conn is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                r = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
                if r is None:
                    conn.execute("COMMIT")
                    return None
                if worker_id is not None and (
                    r["status"] != TaskStatus.RUNNING.value
                    or r["leased_by"] != worker_id
                ):
                    conn.execute("COMMIT")
                    return None
                task = self._row_to_task(r)
                conn.execute(
                    """INSERT OR REPLACE INTO dead_letters
                       (task_id, namespace, queue, payload, attempts, reason, failed_at_ms)
                       VALUES(?,?,?,?,?,?,?)""",
                    (task.id, task.namespace, task.queue, json.dumps(task.payload),
                     task.attempt, reason, now),
                )
                conn.execute(
                    "UPDATE tasks SET status=?, leased_by=NULL, lease_expires_ms=0, "
                    "updated_at_ms=?, last_error=? WHERE id=?",
                    (TaskStatus.DEAD.value, now, reason, task_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return DeadLetter(
                task_id=task.id, namespace=task.namespace, queue=task.queue,
                payload=task.payload, attempts=task.attempt, reason=reason,
                failed_at_ms=now,
            )

    def list_dead_letters(self, namespace: str, queue: str | None = None,
                         limit: int = 100, offset: int = 0) -> list[DeadLetter]:
        assert self._conn is not None
        with self._lock:
            if queue is None:
                rows = self._conn.execute(
                    "SELECT * FROM dead_letters WHERE namespace=? "
                    "ORDER BY failed_at_ms LIMIT ? OFFSET ?",
                    (namespace, limit, offset),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM dead_letters WHERE namespace=? AND queue=? "
                    "ORDER BY failed_at_ms LIMIT ? OFFSET ?",
                    (namespace, queue, limit, offset),
                ).fetchall()
            return [
                DeadLetter(
                    task_id=r["task_id"], namespace=r["namespace"], queue=r["queue"],
                    payload=json.loads(r["payload"]), attempts=r["attempts"],
                    reason=r["reason"], failed_at_ms=r["failed_at_ms"],
                )
                for r in rows
            ]

    def expire_leases(self, now: int, lease_ms: int) -> int:
        """把租约超时的 RUNNING 任务退回 RETRY_WAIT 并立即可重投。

        attempt 不回滚（已发生过的执行次数是事实），run_at 置为 now；
        这样崩溃恢复时不会丢任务，也不会重置重试预算。
        """
        assert self._conn is not None
        with self._lock:
            cur = self._conn.execute(
                """UPDATE tasks
                   SET status=?, leased_by=NULL, lease_expires_ms=0, run_at_ms=?
                   WHERE status=? AND lease_expires_ms > 0 AND lease_expires_ms <= ?""",
                (TaskStatus.RETRY_WAIT.value, now, TaskStatus.RUNNING.value, now),
            )
            return int(cur.rowcount or 0)

    def count_pending(self, namespace: str) -> int:
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT COUNT(*) c FROM tasks WHERE namespace=? "
                "AND status IN (?, ?)",
                (namespace, TaskStatus.PENDING.value, TaskStatus.RETRY_WAIT.value),
            ).fetchone()
            return int(r["c"])

    def count_unfinished(self, namespace: str | None = None) -> int:
        """统计非终态任务数（PENDING/RETRY_WAIT/RUNNING）；namespace=None 为全量。"""
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT COUNT(*) c FROM tasks "
                "WHERE status NOT IN (?, ?, ?) AND (? IS NULL OR namespace=?)",
                (*_TASK_TERMINAL, namespace, namespace),
            ).fetchone()
            return int(r["c"])

    def backlog_bytes(self, namespace: str) -> int:
        """积压内存粗估：待调度任务序列化负载的字节总和（含 RUNNING 占用）。"""
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT COALESCE(SUM(LENGTH(payload)),0) s FROM tasks "
                "WHERE namespace=? AND status NOT IN (?, ?, ?)",
                (namespace, *_TASK_TERMINAL),
            ).fetchone()
            return int(r["s"])

    # ---- 去重窗口 ------------------------------------------------------
    def insert_task_with_dedup(self, task: TaskRecord, window_ms: int,
                               now: int) -> tuple[bool, str]:
        """原子“检查去重窗口 + 插入任务 + 登记窗口”。

        返回 ``(inserted, task_id)``：
        - 窗口内已存在有效记录：``(False, 首次任务 id)``，不插入新任务；
        - 无记录或窗口已过期：删除过期记录，插入新任务并登记窗口，``(True, 新任务 id)``。

        ``BEGIN IMMEDIATE`` 取得写锁，使并发提交同 key 的两个线程串行判定，
        不可能双双通过检查。
        """
        assert self._conn is not None
        assert task.dedup_key is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                r = conn.execute(
                    "SELECT task_id, expires_at_ms FROM dedup_window "
                    "WHERE namespace=? AND dedup_key=?",
                    (task.namespace, task.dedup_key),
                ).fetchone()
                if r is not None and int(r["expires_at_ms"]) > now:
                    conn.execute("COMMIT")
                    return False, str(r["task_id"])
                conn.execute(
                    "DELETE FROM dedup_window WHERE namespace=? AND dedup_key=?",
                    (task.namespace, task.dedup_key),
                )
                conn.execute(
                    """INSERT INTO tasks(id, namespace, queue, payload, status, attempt,
                           dedup_key, created_at_ms, updated_at_ms, run_at_ms,
                           leased_by, lease_expires_ms, last_error)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        task.id, task.namespace, task.queue, json.dumps(task.payload),
                        task.status.value, task.attempt, task.dedup_key,
                        task.created_at_ms, task.updated_at_ms, task.run_at_ms,
                        task.leased_by, task.lease_expires_ms, task.last_error,
                    ),
                )
                conn.execute(
                    "INSERT INTO dedup_window(namespace, dedup_key, task_id, expires_at_ms) "
                    "VALUES(?,?,?,?)",
                    (task.namespace, task.dedup_key, task.id, now + window_ms),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return True, task.id

    def dedup_seen(self, namespace: str, key: str, now: int) -> tuple[bool, str | None]:
        """查询窗口：``(有效命中, 首次任务 id)``；过期按未命中处理但不清理。"""
        assert self._conn is not None
        with self._lock:
            r = self._conn.execute(
                "SELECT task_id, expires_at_ms FROM dedup_window "
                "WHERE namespace=? AND dedup_key=?",
                (namespace, key),
            ).fetchone()
            if r is not None and int(r["expires_at_ms"]) > now:
                return True, str(r["task_id"])
            return False, (str(r["task_id"]) if r is not None else None)

    def dedup_sweep(self, now: int) -> int:
        """清理所有已过期的去重键，返回清理条数。"""
        assert self._conn is not None
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM dedup_window WHERE expires_at_ms <= ?", (now,)
            )
            return int(cur.rowcount or 0)

    # ---- 事件与消费者组 ------------------------------------------------
    def append_event(self, namespace: str, topic: str, payload: dict[str, Any],
                     now: int) -> EventRecord:
        """追加事件；offset 从 0 起，在事务内取 max+1，并发追加不重号。"""
        assert self._conn is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                r = conn.execute(
                    "SELECT COALESCE(MAX(offset), -1) m FROM events "
                    "WHERE namespace=? AND topic=?",
                    (namespace, topic),
                ).fetchone()
                offset = int(r["m"]) + 1
                conn.execute(
                    "INSERT INTO events(namespace, topic, offset, payload, created_at_ms) "
                    "VALUES(?,?,?,?,?)",
                    (namespace, topic, offset, json.dumps(payload), now),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return EventRecord(offset=offset, namespace=namespace, topic=topic,
                               payload=payload, created_at_ms=now)

    def read_events(self, namespace: str, topic: str, offset: int,
                    limit: int) -> list[EventRecord]:
        assert self._conn is not None
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE namespace=? AND topic=? AND offset>=? "
                "ORDER BY offset LIMIT ?",
                (namespace, topic, offset, limit),
            ).fetchall()
            return [self._row_to_event(r) for r in rows]

    @staticmethod
    def _row_to_event(r: sqlite3.Row) -> EventRecord:
        return EventRecord(
            offset=int(r["offset"]), namespace=r["namespace"], topic=r["topic"],
            payload=json.loads(r["payload"]), created_at_ms=int(r["created_at_ms"]),
        )

    def ensure_group(self, namespace: str, topic: str, group: str) -> None:
        assert self._conn is not None
        with self._lock:
            self._ensure_group_locked(self._conn, namespace, topic, group)

    @staticmethod
    def _ensure_group_locked(conn: sqlite3.Connection, namespace: str, topic: str,
                             group: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO consumer_groups(namespace, topic, grp, committed_offset) "
            "VALUES(?,?,?,0)",
            (namespace, topic, group),
        )

    def fetch_events_for_group(self, namespace: str, topic: str, group: str,
                               consumer: str, now: int, limit: int,
                               lease_ms: int) -> list[EventRecord]:
        """原子认领组内未完成、未被他人租用（或租约已过期）的事件。

        - 只认领 ``offset >= committed_offset`` 的事件；
        - 已 ack 的事件不再投递；
        - 租约过期（崩溃/超时）的事件可被重新认领，attempts 累加；
        - 挑选与打标在同一事务，防止多消费者重复认领。
        """
        assert self._conn is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group_locked(conn, namespace, topic, group)
                committed = int(conn.execute(
                    "SELECT committed_offset FROM consumer_groups "
                    "WHERE namespace=? AND topic=? AND grp=?",
                    (namespace, topic, group),
                ).fetchone()["committed_offset"])
                rows = conn.execute(
                    """SELECT e.namespace, e.topic, e.offset, e.payload, e.created_at_ms
                       FROM events e
                       LEFT JOIN group_acks a
                         ON a.namespace=e.namespace AND a.topic=e.topic
                        AND a.grp=? AND a.offset=e.offset
                       WHERE e.namespace=? AND e.topic=? AND e.offset>=?
                         AND (a.status IS NULL
                              OR (a.status='pending' AND a.lease_expires_ms<=?))
                       ORDER BY e.offset LIMIT ?""",
                    (group, namespace, topic, committed, now, limit),
                ).fetchall()
                picked = [int(r["offset"]) for r in rows]
                for off in picked:
                    conn.execute(
                        """INSERT INTO group_acks(namespace, topic, grp, offset, status,
                               leased_by, lease_expires_ms, attempts)
                           VALUES(?,?,?,?, 'pending', ?, ?, 1)
                           ON CONFLICT(namespace, topic, grp, offset) DO UPDATE SET
                               leased_by=excluded.leased_by,
                               lease_expires_ms=excluded.lease_expires_ms,
                               attempts=group_acks.attempts+1""",
                        (namespace, topic, group, off, consumer, now + lease_ms),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return [self._row_to_event(r) for r in rows]

    def commit_offset(self, namespace: str, topic: str, group: str,
                      offset: int, consumer: str, now: int) -> str:
        """原子提交位点。语义：``committed_offset`` 是“下一条待处理 offset”。

        返回判定（可解释，不做静默修正）：

        - ``"committed"``：正常把 committed_offset 从 offset 推进到 offset+1；
        - ``"duplicate"``：``offset < committed_offset``，该位点已确认，幂等接受；
        - ``"regression"``：不可能由重复产生的回退（offset<0），拒绝；
        - ``"gap"``：``offset > committed_offset``（跳位/越位），拒绝；
        - ``"not_leased"``：位点未被本组认领或租约不属于提交者，拒绝。
        """
        assert self._conn is not None
        with self._lock:
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group_locked(conn, namespace, topic, group)
                committed = int(conn.execute(
                    "SELECT committed_offset FROM consumer_groups "
                    "WHERE namespace=? AND topic=? AND grp=?",
                    (namespace, topic, group),
                ).fetchone()["committed_offset"])

                if offset < 0:
                    conn.execute("COMMIT")
                    return "regression"
                if offset < committed:
                    # 已确认位点的重复提交：幂等成功
                    conn.execute("COMMIT")
                    return "duplicate"
                if offset > committed:
                    conn.execute("COMMIT")
                    return "gap"

                ack = conn.execute(
                    "SELECT leased_by, status FROM group_acks "
                    "WHERE namespace=? AND topic=? AND grp=? AND offset=?",
                    (namespace, topic, group, offset),
                ).fetchone()
                if ack is None or ack["leased_by"] != consumer:
                    conn.execute("COMMIT")
                    return "not_leased"

                conn.execute(
                    "UPDATE group_acks SET status='acked', leased_by=NULL, "
                    "lease_expires_ms=0 WHERE namespace=? AND topic=? AND grp=? AND offset=?",
                    (namespace, topic, group, offset),
                )
                conn.execute(
                    "UPDATE consumer_groups SET committed_offset=? "
                    "WHERE namespace=? AND topic=? AND grp=?",
                    (offset + 1, namespace, topic, group),
                )
                conn.execute("COMMIT")
                return "committed"
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def committed_offset(self, namespace: str, topic: str, group: str) -> int:
        """返回该组下一条待处理 offset（无记录时为 0）。"""
        assert self._conn is not None
        with self._lock:
            self._ensure_group_locked(self._conn, namespace, topic, group)
            r = self._conn.execute(
                "SELECT committed_offset FROM consumer_groups "
                "WHERE namespace=? AND topic=? AND grp=?",
                (namespace, topic, group),
            ).fetchone()
            return int(r["committed_offset"])

    def expire_group_leases(self, now: int, lease_ms: int) -> int:
        """把过期的 pending 认领标记为可重新认领（清租约），返回条数。

        attempts 计数保留；fetch 时凭 ``lease_expires_ms<=now`` 自动重投。
        """
        assert self._conn is not None
        with self._lock:
            cur = self._conn.execute(
                "UPDATE group_acks SET lease_expires_ms=0, leased_by=NULL "
                "WHERE status='pending' AND lease_expires_ms>0 AND lease_expires_ms<=?",
                (now,),
            )
            return int(cur.rowcount or 0)
