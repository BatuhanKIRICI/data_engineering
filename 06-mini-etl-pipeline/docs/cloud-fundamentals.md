# Cloud Fundamentals

A conceptual map from a Cloud fundamentals learning session — deliberately not tied to one provider's exact service names, since the goal was understanding the underlying model (compute/storage/database/network/access) rather than memorizing AWS/GCP/Azure specifics.

## The Core Trilogy

```
                CLOUD
                  |
      +-----------+-----------+
      v           v           v
   Compute     Storage     Database
      |           |           |
    (EC2)        (S3)       (RDS)
```

- **Compute**: where code actually runs (CPU/RAM) — the cloud equivalent of running a script on your own machine.
- **Storage** (object storage): holds files as-is — CSV, JSON, Parquet, logs, backups. Doesn't process anything itself; something else has to read and act on what it holds.
- **Database**: structured data, built for querying — not where you dump raw, unshaped data at scale.

Storage != Database: a CSV can live in object storage, but running `GROUP BY` on it means either loading it into a database/warehouse or processing it with something that can read it directly (e.g. Spark).

## Data Lake -> Warehouse -> Lakehouse

```
DATA LAKE
  "Store all kinds of raw data at scale, before you know exactly how you'll use it."

DATA WAREHOUSE
  "Structured, modeled data, ready for analysis and querying."

LAKEHOUSE
  "Object storage with warehouse-like table/transaction/query capabilities on top."
```

Typical flow:
```
Raw data (as it arrives)
    |
    v
RAW        -- stored exactly as received
    |
    v
PROCESSED  -- cleaned, validated
    |
    v
CURATED    -- shaped for a specific analytical use case
    |
    v
Warehouse / Analytics / BI
```

Key reason to land in a lake first: if requirements aren't fully known yet, keeping the raw data means a new question later doesn't require re-collecting data from scratch — only reprocessing what's already stored.

Historical note: HDFS (Hadoop Distributed File System) was the original distributed storage model for this; most new architectures today use cloud object storage (S3 etc.) with Spark for processing instead of a self-managed Hadoop cluster.

## Networking

```
VPC        -- a private network space within the cloud
  |
  +-- Subnet          -- a subdivision of that network (e.g. public vs private)
  |
  +-- Security Group   -- which traffic/ports are allowed in and out
```

Typical shape:
```
Internet
   |
Public Subnet
   |
  EC2 (compute)
   | :5432
Private Subnet
   |
  RDS (database) -- not directly exposed to the internet
```

## IAM (Identity and Access Management)

Answers a different question than networking:

```
Network        -> "Can I even reach it?"
Authentication -> "Who am I?"
Authorization  -> "Am I allowed to do this specific thing here?"
```

Principle: **least privilege** — grant only what's needed (e.g. S3 Read, not Write/Delete) rather than broad/admin access by default.

## Debugging Map (connects back to Linux basics)

A useful way to read a connection error:

```
"connection timed out"          -> likely Network/Security Group (never even reached the resource)
"authentication failed"         -> reached it, but identity/credentials rejected
"permission denied"             -> identity accepted, but not authorized for this action
```

This mirrors the same layered debugging approach used locally with Linux (`ss`, `nc`, `psql`) — just at cloud scale, across separate machines instead of one.
