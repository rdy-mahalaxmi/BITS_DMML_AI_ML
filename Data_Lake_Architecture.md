# Data Lake Storage Structure Documentation

## Overview
This document describes the data storage architecture implemented for the Customer Churn Prediction Pipeline. The storage system uses a local filesystem organized as a data lake with efficient partitioning strategy.

## Storage Architecture

### Root Structure
```
data/
├── raw/           # Initial ingested data (temporary storage)
├── lake/          # Partitioned data lake (main storage)
├── processed/     # Cleaned and prepared datasets
├── db/           # Database files (SQLite)
└── reports/      # Data quality reports and visualizations
```

## Data Lake Partitioning Strategy

### Lake Structure (`data/lake/`)
The data lake implements a **3-level partitioning strategy**:

```
data/lake/
├── {source_type}/          # Level 1: Partition by data source
│   └── {timestamp}/        # Level 2: Partition by ingestion timestamp
│       └── {filename}      # Level 3: Actual data files
```

### Partitioning Levels Explained

#### Level 1: Source Type Partitioning
- **`csv_source/`**: Data ingested from CSV files (Telco dataset)
- **`api_source/`**: Data ingested from REST APIs (JSONPlaceholder)

#### Level 2: Timestamp Partitioning
- **Format**: `YYYYMMDD_HHMMSS` (e.g., `20250824_152847`)
- **Purpose**: Enables time-travel queries and version control
- **Granularity**: Second-level precision for unique pipeline runs

#### Level 3: File Storage
- **CSV Source**: `churn.csv` (customer churn dataset)
- **API Source**: `users.csv` (user demographics data)

## Example Storage Paths

### CSV Data Ingestion
```
data/lake/csv_source/20250824_152847/churn.csv
data/lake/csv_source/20250823_220217/churn.csv
data/lake/csv_source/20250823_150931/churn.csv
```

### API Data Ingestion
```
data/lake/api_source/20250824_152847/users.csv
data/lake/api_source/20250823_220217/users.csv
data/lake/api_source/20250823_150931/users.csv
```

## Storage Benefits

### 1. **Scalability**
- Easy addition of new source types (e.g., `database_source/`, `streaming_source/`)
- Horizontal scaling through timestamp partitioning
- Efficient query performance with partition pruning

### 2. **Data Governance**
- Clear data lineage tracking through source partitioning
- Immutable storage with timestamp-based versioning
- Easy data retention policy implementation

### 3. **Performance Optimization**
- Partition elimination for time-range queries
- Parallel processing capabilities
- Reduced I/O for targeted data access

### 4. **Operational Efficiency**
- Simplified backup and recovery (by source or time range)
- Easy debugging and troubleshooting
- Clear audit trail for compliance

## Storage Statistics

### Current Storage Utilization
- **CSV Source**: 10 timestamped versions
- **API Source**: 10 timestamped versions
- **Total Lake Size**: ~20 data files across 20 time partitions
- **Date Range**: 2025-08-23 to 2025-08-24

### File Size Metrics
- **Churn Dataset**: ~7,043 records per file
- **API Dataset**: ~10 records per file
- **Storage Growth**: Linear with pipeline execution frequency

## Implementation Details

### Automated Partitioning
```python
def store_file(filename, source_type):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_dir = os.path.join(STORAGE_DIR, source_type, timestamp)
    os.makedirs(source_dir, exist_ok=True)
    # Copy file to partitioned location
```

### Path Construction
```python
STORAGE_DIR = "data/lake"
source_path = f"{STORAGE_DIR}/{source_type}/{timestamp}/{filename}"
```

## Query Patterns

### Time-Range Queries
```python
# Get data for specific date
date_pattern = "data/lake/*/20250824_*/*.csv"

# Get latest data
latest_folder = max(os.listdir(f"data/lake/{source_type}"))
```

### Source-Specific Queries
```python
# Get all CSV source data
csv_data = "data/lake/csv_source/**/*.csv"

# Get all API source data
api_data = "data/lake/api_source/**/*.csv"
```

## Maintenance Operations

### Data Retention
- **Policy**: Keep all historical versions for reproducibility
- **Cleanup**: Manual cleanup of old partitions if needed
- **Archival**: Move old partitions to archival storage

### Monitoring
- **Storage Growth**: Monitor partition count and file sizes
- **Performance**: Track query performance across partitions
- **Health Checks**: Validate partition structure integrity

## Future Enhancements

### Planned Improvements
1. **Compression**: Implement file compression for older partitions
2. **Metadata**: Add partition metadata files for better governance
3. **Indexing**: Create partition indexes for faster queries
4. **Cloud Migration**: Design for easy migration to cloud storage (S3, GCS)

### Scalability Considerations
- **Hot/Cold Storage**: Move older partitions to cheaper storage tiers
- **Global Partitioning**: Add geographic partitioning for multi-region data
- **Format Evolution**: Support for Parquet/ORC formats for better performance

---

**Document Version**: 1.0  
**Last Updated**: August 24, 2025  
**Implementation**: Local Filesystem Data Lake  
**Partitioning Strategy**: Source + Timestamp  
**Status**: Production Ready ✅
