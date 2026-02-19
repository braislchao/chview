"""Core data models for CHView."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TableInfo:
    """Information about a database table."""

    database: str
    name: str
    engine: str
    total_rows: Optional[int] = None
    total_bytes: Optional[int] = None

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.name}"


@dataclass
class ClusterInfo:
    """ClickHouse cluster overview information."""

    version: str
    uptime_seconds: int
    user_databases: int
    user_tables: int
    mv_count: int
    total_disk_bytes: Optional[int] = None

    @property
    def uptime_display(self) -> str:
        """Return human-readable uptime."""
        hours = self.uptime_seconds / 3600
        if hours >= 24:
            return f"{hours / 24:.1f} days"
        return f"{hours:.1f} hrs"


@dataclass
class StorageMetrics:
    """Storage metrics for a table."""

    database: str
    table: str
    rows: int
    bytes_on_disk: int
    compressed_bytes: int
    uncompressed_bytes: Optional[int] = None

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.table}"

    @property
    def compression_ratio(self) -> Optional[float]:
        """Calculate compression ratio if uncompressed bytes available."""
        if self.uncompressed_bytes and self.compressed_bytes > 0:
            return self.uncompressed_bytes / self.compressed_bytes
        return None


@dataclass
class PartitionMetrics:
    """Storage metrics for a partition."""

    database: str
    table: str
    partition: str
    rows: int
    bytes_on_disk: int
    compressed_bytes: int


@dataclass
class MaterializedViewInfo:
    """Information about a materialized view."""

    database: str
    name: str
    create_query: str
    dependencies_database: list[str] = field(default_factory=list)
    dependencies_table: list[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        """Return fully qualified view name."""
        return f"{self.database}.{self.name}"


@dataclass
class ThroughputMetrics:
    """Materialized view throughput metrics."""

    view_name: str
    interval_start: str
    executions: int
    rows_read: int
    rows_written: int
    bytes_written: int
    avg_duration_ms: float


@dataclass
class MVError:
    """Materialized view error entry."""

    view_name: str
    exception_code: int
    exception: str
    event_time: str
    error_count: int


@dataclass
class KafkaConsumerInfo:
    """Kafka consumer health information."""

    database: str
    table: str
    consumer_id: str
    topic: str
    partition_id: int
    current_offset: int
    last_poll_time: str
    num_messages_read: int
    rebalance_count: int
    is_currently_used: bool
    seconds_since_poll: int

    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.table}"

    @property
    def health_status(self) -> str:
        """Determine health status based on poll time."""
        if not self.is_currently_used:
            return "inactive"
        if self.seconds_since_poll > 300:
            return "error"
        if self.seconds_since_poll > 60:
            return "warning"
        return "healthy"


@dataclass
class ConnectionInfo:
    """ClickHouse connection information."""

    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool


@dataclass
class LineageNode:
    """Node in the lineage graph."""

    database: str
    name: str
    engine: str
    full_name: str = ""

    def __post_init__(self):
        if not self.full_name:
            self.full_name = f"{self.database}.{self.name}"


@dataclass
class LineageEdge:
    """Edge in the lineage graph."""

    source: str
    target: str
    mv_name: str


@dataclass
class LineageGraph:
    """Complete lineage graph."""

    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)
    mv_names: set[str] = field(default_factory=set)
    target_names: set[str] = field(default_factory=set)
