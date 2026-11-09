package signals

// pgDatabaseDatnameSel excludes template databases on postgres_exporter series.
const pgDatabaseDatnameSel = `,datname!=""`

// pgUserTablePrimarySel scopes user tables on the primary writer pod (not replicas).
const pgUserTablePrimarySel = `,datname!~"template0|template1",relname!~"_c9_.*",pod=~"postgres.*",pod!~"postgres-replica.*"`
